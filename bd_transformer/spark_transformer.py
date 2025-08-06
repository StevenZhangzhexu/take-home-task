from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, substring, length, round as spark_round, concat, min as spark_min, max as spark_max, avg, stddev
from pyspark.sql.types import DoubleType, StringType

import bd_transformer.consts as const
from bd_transformer.spark_components.converter import SparkConverter
from bd_transformer.spark_components.normalizer import SparkNormalizer

class SparkTransformer:
    """
    Spark-based transformer - Optimized version with merged column processing
    """
    def __init__(self, config: dict, spark: SparkSession = None):
        self.config = config
        self.converters = {}
        self.normalizers = {}
        self.spark = spark or SparkSession.builder.appName("BDTransformer").getOrCreate()

    def fit(self, data: DataFrame) -> "SparkTransformer":
        # Optimize: Sample all columns at once instead of separate operations
        sample_cols = [col(c) for c in self.config.keys()]
        sample_data = data.select(*sample_cols).limit(1).collect()[0]
        
        # Determine column types from single sample
        string_cols = []
        numeric_cols = []
        
        for i, col_name in enumerate(self.config.keys()):
            if isinstance(sample_data[i], str):
                string_cols.append(col_name)
            else:
                numeric_cols.append(col_name)
        
        # OPTIMIZATION: Single Spark job for all column statistics (numeric + string)
        all_stats = {}
        all_agg_exprs = []
        
        # Add numeric column expressions directly
        for col_name in numeric_cols:
            all_agg_exprs.extend([
                spark_min(col_name).alias(f"{col_name}_min"),
                spark_max(col_name).alias(f"{col_name}_max")
            ])
        
        # Add string column expressions with conversion
        for col_name in string_cols:
            conv_config = self.config[col_name].get("converter", {})
            col_expr = col(col_name)
            
            # Apply string-to-number conversion inline
            if conv_config.get('prefix'):
                col_expr = substring(col_expr, len(conv_config['prefix']) + 1, length(col_expr))
            if conv_config.get('suffix'):
                col_expr = substring(col_expr, 1, length(col_expr) - len(conv_config['suffix']))
            col_expr = col_expr.cast(DoubleType())
            
            all_agg_exprs.extend([
                spark_min(col_expr).alias(f"{col_name}_min"),
                spark_max(col_expr).alias(f"{col_name}_max")
            ])
        
        # Single Spark job for all statistics
        if all_agg_exprs:
            stats_result = data.agg(*all_agg_exprs).collect()[0]
            
            # Store statistics for all columns
            for col_name in self.config.keys():
                all_stats[col_name] = {
                    'min': stats_result[f"{col_name}_min"],
                    'max': stats_result[f"{col_name}_max"]
                }
        
        # Process all columns using the collected statistics
        col_to_index = {col_name: i for i, col_name in enumerate(self.config.keys())}
        
        for col_name in self.config:
            # Create converter with extracted statistics
            conv_config = self.config[col_name].get("converter", {}).copy()
            
            # Use empirical min/max if specified as True
            if conv_config.get('min_val') is True:
                conv_config['min_val'] = all_stats[col_name]['min']
            if conv_config.get('max_val') is True:
                conv_config['max_val'] = all_stats[col_name]['max']
            
            # Create converter and set attributes directly (no Spark operations)
            converter = SparkConverter(**conv_config, spark=self.spark)
            converter._type = type(sample_data[col_to_index[col_name]])
            converter._column_name = col_name
            
            # Handle min_val parsing
            if isinstance(converter._min_val, bool) and converter._min_val:
                converter._min_val = all_stats[col_name]['min']
            elif isinstance(converter._min_val, str):
                if converter._prefix:
                    converter._min_val = converter._min_val[len(converter._prefix):]
                if converter._suffix:
                    converter._min_val = converter._min_val[:-len(converter._suffix)]
                converter._min_val = float(converter._min_val)
            elif isinstance(converter._min_val, bool) and not converter._min_val:
                converter._min_val = float('-inf')
            
            # Handle max_val parsing
            if isinstance(converter._max_val, bool) and converter._max_val:
                converter._max_val = all_stats[col_name]['max']
            elif isinstance(converter._max_val, str):
                if converter._prefix:
                    converter._max_val = converter._max_val[len(converter._prefix):]
                if converter._suffix:
                    converter._max_val = converter._max_val[:-len(converter._suffix)]
                converter._max_val = float(converter._max_val)
            elif isinstance(converter._max_val, bool) and not converter._max_val:
                converter._max_val = float('inf')
            
            self.converters[col_name] = converter
            
            # Create normalizer and set attributes directly
            norm_config = self.config[col_name].get("normalizer", {}).copy()
            normalizer = SparkNormalizer(**norm_config, spark=self.spark)
            normalizer._column_name = col_name
            
            # Use the statistics we already collected
            normalizer._min = float(all_stats[col_name]['min'])
            normalizer._max = float(all_stats[col_name]['max'])
            normalizer._scale = normalizer._max - normalizer._min
            normalizer._scale = 1.0 if normalizer._scale == 0 else normalizer._scale
            
            self.normalizers[col_name] = normalizer
        
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

            # String to number (if needed)
            if params['conv_type'] == 'str':
                if params['conv_prefix']:
                    col_expr = substring(col_expr, len(params['conv_prefix']) + 1, length(col_expr))
                if params['conv_suffix']:
                    col_expr = substring(col_expr, 1, length(col_expr) - len(params['conv_suffix']))
                col_expr = col_expr.cast(DoubleType())

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
        
        # Apply all transformations in a single select operation
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
            if params['conv_type'] == 'str':
                col_expr = col_expr.cast(StringType())
                if params['conv_prefix']:
                    col_expr = concat(lit(params['conv_prefix']), col_expr)
                if params['conv_suffix']:
                    col_expr = concat(col_expr, lit(params['conv_suffix']))
            elif params['conv_type'] in ['int', 'int64', 'int32']:
                col_expr = col_expr.cast("long")
            else:
                col_expr = col_expr.cast(DoubleType())

            select_expressions.append(col_expr.alias(col_name))
        
        # Apply all transformations in a single select operation
        return data.select(*select_expressions)