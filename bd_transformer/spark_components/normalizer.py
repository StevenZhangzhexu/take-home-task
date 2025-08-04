from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, min as spark_min, max as spark_max
from pyspark.sql.types import DoubleType, BooleanType, StringType
import numpy as np

import bd_transformer.consts as const


class SparkNormalizer:
    def __init__(self, clip: bool = False, reject: bool = False, spark: SparkSession = None):
        """
        Parameters
        ----------
        clip : bool = False
            Whether to clip values between 0 and 1. Otherwise, if input values to be inversely transformed are out of
            the range [0, 1], and/or if input values to be transformed are out of range from fit, the values will be
            clipped.
        reject : bool = False
            Whether to mark inputs out of [0, 1] as invalid when inverse transformation.
        """
        self._clip = clip
        self._reject = reject
        self.spark = spark

        self._min = None
        self._max = None
        self._scale = None
        self._column_name = None

    def fit(self, data: DataFrame) -> "SparkNormalizer":
        self._column_name = data.columns[0]
        
        # Calculate min and max
        stats = data.agg(
            spark_min(self._column_name).alias("min_val"),
            spark_max(self._column_name).alias("max_val")
        ).collect()[0]
        
        self._min = stats["min_val"]
        self._max = stats["max_val"]
        self._scale = self._max - self._min
        self._scale = 1 if self._scale == 0 else self._scale
        return self

    def normalize(self, data: DataFrame) -> DataFrame:
        if self._clip:
            data = data.withColumn(
                self._column_name,
                when(col(self._column_name) < self._min, self._min)
                .when(col(self._column_name) > self._max, self._max)
                .otherwise(col(self._column_name))
            )
        
        data = data.withColumn(
            self._column_name,
            (col(self._column_name) - self._min) / self._scale
        )
        return data

    def inverse_normalize(self, data: DataFrame) -> DataFrame:
        # Create valid and error columns
        data = data.withColumn(f"{self._column_name}_{const.VALID_COL_NAME}", lit(True))
        data = data.withColumn(f"{self._column_name}_{const.ERROR_COL_NAME}", lit(""))

        if self._clip:
            data = data.withColumn(
                self._column_name,
                when(col(self._column_name) < 0, 0)
                .when(col(self._column_name) > 1, 1)
                .otherwise(col(self._column_name))
            )
        
        if self._reject:
            # Mark out of range values as invalid
            data = data.withColumn(
                f"{self._column_name}_{const.VALID_COL_NAME}",
                when((col(self._column_name) < 0) | (col(self._column_name) > 1), False)
                .otherwise(True)
            )
            data = data.withColumn(
                f"{self._column_name}_{const.ERROR_COL_NAME}",
                when((col(self._column_name) < 0) | (col(self._column_name) > 1), "out of range [0,1]")
                .otherwise("")
            )
        
        # inverse
        data = data.withColumn(
            self._column_name,
            (col(self._column_name) * self._scale) + self._min
        )

        # Set invalid values to null
        data = data.withColumn(
            self._column_name,
            when(col(f"{self._column_name}_{const.VALID_COL_NAME}"), col(self._column_name))
        )

        return data 