from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, substring, length, round as spark_round, concat
from pyspark.sql.types import DoubleType, StringType

import bd_transformer.consts as const
from bd_transformer.spark_components.converter import SparkConverter
from bd_transformer.spark_components.normalizer import SparkNormalizer

class SparkTransformer:
    """
    Spark-based transformer
    """
    def __init__(self, config: dict, spark: SparkSession = None):
        self.config = config
        self.converters = {}
        self.normalizers = {}
        self.spark = spark or SparkSession.builder.appName("BDTransformer").getOrCreate()

    def fit(self, data: DataFrame) -> "SparkTransformer":
        for col_name in self.config:
            self.converters[col_name] = SparkConverter(
                **self.config[col_name].get("converter", {}),
                spark=self.spark
            ).fit(data.select(col_name))
            converted = self.converters[col_name].convert(data.select(col_name))
            self.normalizers[col_name] = SparkNormalizer(
                **self.config[col_name].get("normalizer", {}),
                spark=self.spark
            ).fit(converted)
        return self

    def transform(self, data: DataFrame) -> DataFrame:
        result_df = data
        for col_name in self.config:
            conv = self.converters[col_name]
            norm = self.normalizers[col_name]
            col_expr = col(col_name)

            # String to number (if needed)
            if conv._type == str:
                if conv._prefix:
                    col_expr = substring(col_expr, len(conv._prefix) + 1, length(col_expr))
                if conv._suffix:
                    col_expr = substring(col_expr, 1, length(col_expr) - len(conv._suffix))
                col_expr = col_expr.cast(DoubleType())

            # Clip (if needed)
            if conv._clip_oor:
                col_expr = when(col_expr < conv._min_val, conv._min_val) \
                    .when(col_expr > conv._max_val, conv._max_val) \
                    .otherwise(col_expr)

            # Normalize
            if norm._clip:
                col_expr = when(col_expr < norm._min, norm._min) \
                    .when(col_expr > norm._max, norm._max) \
                    .otherwise(col_expr)
            col_expr = (col_expr - norm._min) / norm._scale

            result_df = result_df.withColumn(col_name, col_expr)
        return result_df

    def inverse_transform(self, data: DataFrame) -> DataFrame:
        result_df = data
        for col_name in self.config:
            conv = self.converters[col_name]
            norm = self.normalizers[col_name]
            col_expr = col(col_name)

            # Inverse normalize
            col_expr = (col_expr * norm._scale) + norm._min

            # Clip (if needed)
            if norm._clip:
                col_expr = when(col_expr < norm._min, norm._min) \
                    .when(col_expr > norm._max, norm._max) \
                    .otherwise(col_expr)

            # Inverse convert: round, cast, add prefix/suffix if needed
            if conv._rounding is not None:
                col_expr = spark_round(col_expr, conv._rounding)
            if conv._type == str:
                col_expr = col_expr.cast(StringType())
                if conv._prefix:
                    col_expr = concat(lit(conv._prefix), col_expr)
                if conv._suffix:
                    col_expr = concat(col_expr, lit(conv._suffix))
            elif conv._type is not None:
                # Cast back to original type if needed (e.g., int)
                if str(conv._type).startswith("<class 'int"):
                    col_expr = col_expr.cast("long")
                else:
                    col_expr = col_expr.cast(DoubleType())

            result_df = result_df.withColumn(col_name, col_expr)
        return result_df