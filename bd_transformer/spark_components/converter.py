from typing import Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, round, substring, length
from pyspark.sql.types import StringType, DoubleType, FloatType
import numpy as np

import bd_transformer.consts as const


def string_to_numbers_spark(
    data: DataFrame,
    column_name: str,
    prefix: str = "",
    suffix: str = "",
) -> DataFrame:
    """
    Convert a Spark DataFrame column of strings to numbers, handling prefix and suffix.
    """
    result = data
    if prefix:
        result = result.withColumn(
            column_name,
            substring(col(column_name), len(prefix) + 1, length(col(column_name)))
        )
    if suffix:
        result = result.withColumn(
            column_name,
            substring(col(column_name), 1, length(col(column_name)) - len(suffix)))
    
    # Convert to double
    result = result.withColumn(column_name, col(column_name).cast(DoubleType()))
    return result


def number_to_strings_spark(
    data: DataFrame,
    column_name: str,
    prefix: str = "",
    suffix: str = "",
) -> DataFrame:
    """
    Convert a Spark DataFrame column of numbers to strings, handling prefix and suffix.
    """
    result = data.withColumn(column_name, col(column_name).cast(StringType()))
    
    if prefix or suffix:
        # Concatenate prefix and suffix in a single step
        result = result.withColumn(column_name, concat(lit(prefix), col(column_name), lit(suffix)))
    
    return result


class SparkConverter:
    def __init__(
        self,
        min_val: Union[str, int, float, bool] = False,
        max_val: Union[str, int, float, bool] = False,
        clip_oor: bool = True,
        prefix: str = "",
        suffix: str = "",
        rounding: int = 6,
        spark: SparkSession = None,
    ):
        """
        Parameters
        ----------
        min_val : number | bool = False
            The minimum value of the column. Any value smaller than the minimum value will be clipped to the minimum
            value. It can be:
            - A numeric value indicating the actual minimum value allowed.
            - A string in the same prefix, and suffix as this column that can be also understood as
              a number.
            - Boolean True means use the empirical minimum value in the input data. False means no minimum value is
              used (so no clipping in the minimum value side is applied).
        max_val : number | bool = False
            The maximum value of the column. Any value larger than the maximum value will be clipped to the maximum
            value. The value formats are similar to `min_val`.
        clip_oor : bool = True
            Whether to clip out-of-range values. If True, values passed to convert() or inverse_convert() that fall
            outside the range determined during fit() (as defined by min_val and max_val) will be clipped to that range.
            If False, convert() and inverse_convert() will proceed without clipping and mark inputs to inverse_convert()
            that are out of range as invalid.
        prefix : str = ""
            The prefix of the string representation of the values. For typical numbers, nothing need to be given.
            But this parameter is useful when the data represent currencies, and one can specify the currency in front
            if it is present, for example, "$". Note that we will not do stripping automatically, so if the values are
            written as "$ 12", then the prefix is actually "$ ". We only accept uniform prefix that is applied to all
            values in the column.
        suffix : str = ""
            The suffix of the string representation of the values. For typical numbers, nothing need to be given.
            But this parameter is useful when the data come with units, like 12ms, or in percentages, like 23.5%, which
            have a suffix of "ms" and "%" respectively. We only accept uniform suffix that is applied to all values
            in the column.
        rounding : int = 6
            The number of decimal places to round the values to during inverse_convert().
        """
        self._min_val = min_val
        self._max_val = max_val
        self._clip_oor = clip_oor
        self._prefix = prefix
        self._suffix = suffix
        self._rounding = rounding
        self.spark = spark

        self._type = None
        self._column_name = None

    def fit(self, data: DataFrame) -> "SparkConverter":
        self._column_name = data.columns[0]
        
        # Determine data type more efficiently
        sample_row = data.select(self._column_name).limit(1).first()[0]
        self._type = type(sample_row)
        
        # Convert string data to numbers for fitting
        if self._type == str:
            data = string_to_numbers_spark(data, self._column_name, self._prefix, self._suffix)
        
        # Cache data for multiple aggregations
        data = data.cache()
        
        if isinstance(self._min_val, bool) and self._min_val:
            min_result = data.agg({self._column_name: "min"}).first()
            self._min_val = min_result[0]
        elif isinstance(self._min_val, str):
            if self._prefix:
                self._min_val = self._min_val[len(self._prefix):]
            if self._suffix:
                self._min_val = self._min_val[:-len(self._suffix)]
            self._min_val = float(self._min_val)
        elif isinstance(self._min_val, bool) and not self._min_val:
            self._min_val = float('-inf')

        if isinstance(self._max_val, bool) and self._max_val:
            max_result = data.agg({self._column_name: "max"}).first()
            self._max_val = max_result[0]
        elif isinstance(self._max_val, str):
            # Parse string value
            if self._prefix:
                self._max_val = self._max_val[len(self._prefix):]
            if self._suffix:
                self._max_val = self._max_val[:-len(self._suffix)]
            self._max_val = float(self._max_val)
        elif isinstance(self._max_val, bool) and not self._max_val:
            self._max_val = float('inf')
        
        # Unpersist cached data
        data.unpersist()
        return self

    def convert(self, data: DataFrame) -> DataFrame:
        if self._type == str:
            data = string_to_numbers_spark(data, self._column_name, self._prefix, self._suffix)
        
        if self._clip_oor:
            data = data.withColumn(
                self._column_name,
                when(col(self._column_name) < self._min_val, self._min_val)
                .when(col(self._column_name) > self._max_val, self._max_val)
                .otherwise(col(self._column_name))
            )
        return data

    def inverse_convert(self, data: DataFrame) -> DataFrame:
        # Create valid and error columns
        data = data.withColumn(f"{self._column_name}_{const.VALID_COL_NAME}", lit(True))
        data = data.withColumn(f"{self._column_name}_{const.ERROR_COL_NAME}", lit(""))

        if self._clip_oor:
            data = data.withColumn(
                self._column_name,
                when(col(self._column_name) < self._min_val, self._min_val)
                .when(col(self._column_name) > self._max_val, self._max_val)
                .otherwise(col(self._column_name))
            )
        else:
            # Mark out of range values as invalid
            data = data.withColumn(
                f"{self._column_name}_{const.VALID_COL_NAME}",
                when((col(self._column_name) < self._min_val) | (col(self._column_name) > self._max_val), False)
                .otherwise(True)
            )
            data = data.withColumn(
                f"{self._column_name}_{const.ERROR_COL_NAME}",
                when((col(self._column_name) < self._min_val) | (col(self._column_name) > self._max_val), "Out of range")
                .otherwise("")
            )

        data = data.withColumn(
            self._column_name,
            round(col(self._column_name), self._rounding)
        )

        if self._type == str:
            data = number_to_strings_spark(data, self._column_name, self._prefix, self._suffix)

        # Set invalid values to null
        data = data.withColumn(
            self._column_name,
            when(col(f"{self._column_name}_{const.VALID_COL_NAME}"), col(self._column_name))
        )

        return data 