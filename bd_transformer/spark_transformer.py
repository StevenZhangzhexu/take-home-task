from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, substring, length, round as spark_round, concat
from pyspark.sql.functions import min as spark_min, max as spark_max
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DoubleType, StringType

import bd_transformer.consts as const
from bd_transformer.spark_components.converter import SparkConverter
from bd_transformer.spark_components.normalizer import SparkNormalizer

class SparkTransformer:
    """
    Spark-based transformer optimized for single Spark job execution
    """
    def __init__(self, config: dict, spark: SparkSession = None):
        self.config = config
        self.converters = {}
        self.normalizers = {}
        self.spark = spark or SparkSession.builder.appName("BDTransformer").getOrCreate()

    def _safe_string_to_double_expr(self, col_expr, prefix="", suffix=""):
        """Create a safe string-to-double conversion expression that handles errors gracefully"""
        # Apply prefix/suffix removal
        if prefix:
            col_expr = substring(col_expr, len(prefix) + 1, length(col_expr))
        if suffix:
            col_expr = substring(col_expr, 1, length(col_expr) - len(suffix))
        
        # Handle empty strings
        col_expr = when(length(col_expr) > 0, col_expr).otherwise(None)
        
        # Safe conversion: use try_cast if available, otherwise use regex validation
        try:
            from pyspark.sql.functions import try_cast
            return try_cast(col_expr, DoubleType())
        except ImportError:
            # Fallback: validate with regex before casting
            # This regex matches valid numeric values including decimals, negatives, scientific notation
            numeric_pattern = r'^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$'
            return when(
                regexp_replace(col_expr, r'\s', '').rlike(numeric_pattern),
                col_expr.cast(DoubleType())
            ).otherwise(None)

    def fit(self, data: DataFrame) -> "SparkTransformer":
        # Step 1: Prepare SparkConverters for each column and build conversion expressions
        converters = {}
        converted_exprs = []
        
        for col_name in self.config:
            conv = SparkConverter(
                **self.config[col_name].get("converter", {}),
                spark=self.spark
            )
            converters[col_name] = conv
            
            # Get the original column data type
            original_type = data.select(col_name).schema[0].dataType
            conv._type = original_type.__class__
            conv._column_name = col_name
            
            # Build converted column expression with safe error handling
            col_expr = col(col_name)
            
            # Only apply string processing if it's actually a string column
            if conv._type.__name__ in ['StringType', 'str']:
                col_expr = self._safe_string_to_double_expr(col_expr, conv._prefix, conv._suffix)
            else:
                # For non-string columns, just cast to double
                col_expr = col_expr.cast(DoubleType())
            
            converted_exprs.append(col_expr.alias(f"__conv_{col_name}"))

        # Step 2: Create DataFrame with all converted columns (SINGLE Spark job)
        converted_df = data.select(*[col(c) for c in data.columns], *converted_exprs)

        # Step 3: Aggregate min/max for all converted columns in one go (SINGLE Spark job)
        agg_exprs = []
        for col_name in self.config:
            conv_col = f"__conv_{col_name}"
            agg_exprs.append(spark_min(conv_col).alias(f"{col_name}_min"))
            agg_exprs.append(spark_max(conv_col).alias(f"{col_name}_max"))
        
        stats_row = converted_df.agg(*agg_exprs).collect()[0]

        # Step 4: Initialize converters and normalizers using collected stats (NO Spark jobs)
        for col_name in self.config:
            conv = converters[col_name]
            min_val = stats_row[f"{col_name}_min"]
            max_val = stats_row[f"{col_name}_max"]

            # Handle min_val configuration
            if conv._min_val is True:
                conv._min_val = min_val if min_val is not None else float('-inf')
            elif isinstance(conv._min_val, str):
                parsed_min = conv._min_val
                if conv._prefix:
                    parsed_min = parsed_min[len(conv._prefix):]
                if conv._suffix:
                    parsed_min = parsed_min[:-len(conv._suffix)]
                try:
                    conv._min_val = float(parsed_min)
                except ValueError:
                    conv._min_val = float('-inf')
            elif isinstance(conv._min_val, bool) and not conv._min_val:
                conv._min_val = float('-inf')

            # Handle max_val configuration
            if conv._max_val is True:
                conv._max_val = max_val if max_val is not None else float('inf')
            elif isinstance(conv._max_val, str):
                parsed_max = conv._max_val
                if conv._prefix:
                    parsed_max = parsed_max[len(conv._prefix):]
                if conv._suffix:
                    parsed_max = parsed_max[:-len(conv._suffix)]
                try:
                    conv._max_val = float(parsed_max)
                except ValueError:
                    conv._max_val = float('inf')
            elif isinstance(conv._max_val, bool) and not conv._max_val:
                conv._max_val = float('inf')

            self.converters[col_name] = conv

            # Fit normalizer using collected stats
            norm = SparkNormalizer(**self.config[col_name].get("normalizer", {}), spark=self.spark)
            norm._column_name = col_name
            norm._min = float(min_val) if min_val is not None else 0.0
            norm._max = float(max_val) if max_val is not None else 1.0
            norm._scale = norm._max - norm._min
            norm._scale = 1.0 if norm._scale == 0 else norm._scale

            self.normalizers[col_name] = norm

        return self

    def transform(self, data: DataFrame) -> DataFrame:
        # Extract all primitive values upfront to avoid any object serialization
        transform_params = {}
        for col_name in self.config:
            conv = self.converters[col_name]
            norm = self.normalizers[col_name]
            transform_params[col_name] = {
                'conv_type': conv._type.__name__ if hasattr(conv._type, '__name__') else 'unknown',
                'conv_prefix': conv._prefix,
                'conv_suffix': conv._suffix,
                'conv_clip_oor': conv._clip_oor,
                'conv_min_val': conv._min_val,
                'conv_max_val': conv._max_val,
                'norm_clip': norm._clip,
                'norm_min': norm._min,
                'norm_max': norm._max,
                'norm_scale': norm._scale
            }
        
        # Build all column expressions using only primitive values
        select_expressions = []
        
        # Add unchanged columns
        unchanged_cols = [col(c) for c in data.columns if c not in self.config]
        select_expressions.extend(unchanged_cols)
        
        # Build expressions for configured columns using only primitive values
        for col_name, params in transform_params.items():
            col_expr = col(col_name)

            # String to number conversion (if needed) - using safe conversion
            if params['conv_type'] in ['StringType', 'str']:
                col_expr = self._safe_string_to_double_expr(
                    col_expr, 
                    params['conv_prefix'], 
                    params['conv_suffix']
                )

            # Clip (if needed)
            if params['conv_clip_oor']:
                col_expr = when(col_expr < params['conv_min_val'], params['conv_min_val']) \
                    .when(col_expr > params['conv_max_val'], params['conv_max_val']) \
                    .otherwise(col_expr)

            # Normalize
            if params['norm_clip']:
                col_expr = when(col_expr < params['norm_min'], params['norm_min']) \
                    .when(col_expr > params['norm_max'], params['norm_max']) \
                    .otherwise(col_expr)
            col_expr = (col_expr - params['norm_min']) / params['norm_scale']

            select_expressions.append(col_expr.alias(col_name))
        
        # Apply all transformations in a SINGLE select operation
        return data.select(*select_expressions)

    def inverse_transform(self, data: DataFrame) -> DataFrame:
        # Extract all primitive values upfront to avoid any object serialization
        inverse_params = {}
        for col_name in self.config:
            conv = self.converters[col_name]
            norm = self.normalizers[col_name]
            inverse_params[col_name] = {
                'conv_type': conv._type.__name__ if hasattr(conv._type, '__name__') else 'unknown',
                'conv_prefix': conv._prefix,
                'conv_suffix': conv._suffix,
                'conv_rounding': conv._rounding,
                'norm_clip': norm._clip,
                'norm_min': norm._min,
                'norm_max': norm._max,
                'norm_scale': norm._scale
            }
        
        # Build all column expressions using only primitive values
        select_expressions = []
        
        # Add unchanged columns
        unchanged_cols = [col(c) for c in data.columns if c not in self.config]
        select_expressions.extend(unchanged_cols)
        
        # Build expressions for configured columns using only primitive values
        for col_name, params in inverse_params.items():
            col_expr = col(col_name)

            # Inverse normalize
            col_expr = (col_expr * params['norm_scale']) + params['norm_min']

            # Clip (if needed)
            if params['norm_clip']:
                col_expr = when(col_expr < params['norm_min'], params['norm_min']) \
                    .when(col_expr > params['norm_max'], params['norm_max']) \
                    .otherwise(col_expr)

            # Inverse convert: round, cast, add prefix/suffix if needed
            if params['conv_rounding'] is not None:
                col_expr = spark_round(col_expr, params['conv_rounding'])
            
            if params['conv_type'] in ['StringType', 'str']:
                col_expr = col_expr.cast(StringType())
                if params['conv_prefix']:
                    col_expr = concat(lit(params['conv_prefix']), col_expr)
                if params['conv_suffix']:
                    col_expr = concat(col_expr, lit(params['conv_suffix']))
            elif params['conv_type'] in ['int', 'int64', 'int32', 'IntegerType', 'LongType']:
                col_expr = col_expr.cast("long")
            else:
                col_expr = col_expr.cast(DoubleType())

            select_expressions.append(col_expr.alias(col_name))
        
        # Apply all transformations in a SINGLE select operation
        return data.select(*select_expressions)