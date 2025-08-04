from typing import Union

import numpy as np
import pandas as pd

import bd_transformer.consts as const


def string_to_numbers(
    data: pd.Series,
    prefix: str = "",
    suffix: str = "",
) -> pd.Series:
    """
    Convert a Series of strings to numbers, handling prefix and suffix.
    """
    prefix_len = len(prefix)
    if prefix_len > 0:
        data = data.str.slice(start=prefix_len)
    suffix_len = len(suffix)
    if suffix_len > 0:
        data = data.str.slice(stop=-suffix_len)
    data = data.astype(float)
    return data


def single_string_to_number(
    data: str,
    prefix: str = "",
    suffix: str = "",
) -> Union[float, int]:
    """
    Convert a string to number, handling prefix and suffix.
    """
    prefix_len = len(prefix)
    suffix_len = len(suffix)
    data = data[prefix_len:-suffix_len]
    data = data.astype(float)
    return data


def number_to_strings(
    data: pd.Series,
    prefix: str = "",
    suffix: str = "",
) -> pd.Series:
    """
    Convert a Series of numbers to strings, handling prefix and suffix.
    """
    data = data.astype(str)
    data = prefix + data + suffix
    return data


class Converter:
    def __init__(
        self,
        min_val: Union[str, int, float, bool] = False,
        max_val: Union[str, int, float, bool] = False,
        clip_oor: bool = True,
        prefix: str = "",
        suffix: str = "",
        rounding: int = 6,
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

        self._type = None

    def fit(self, data: pd.Series) -> "Converter":
        self._type = data.dtype
        if data.dtype == "object":
            data = string_to_numbers(data, self._prefix, self._suffix)
        if isinstance(self._min_val, bool) and self._min_val:
            self._min_val = data.min()
        elif isinstance(self._min_val, str):
            self._min_val = single_string_to_number(
                self._min_val,
                self._prefix,
                self._suffix,
            )
        elif isinstance(self._min_val, bool) and not self._min_val:
            self._min_val = -np.inf

        if isinstance(self._max_val, bool) and self._max_val:
            self._max_val = data.max()
        elif isinstance(self._max_val, str):
            self._max_val = single_string_to_number(
                self._max_val,
                self._prefix,
                self._suffix,
            )
        elif isinstance(self._max_val, bool) and not self._max_val:
            self._max_val = np.inf
        return self

    def convert(self, data: pd.Series) -> pd.Series:
        if data.dtype == "object":
            data = string_to_numbers(data, self._prefix, self._suffix)
        if self._clip_oor:
            data = data.clip(self._min_val, self._max_val)
        return data

    def inverse_convert(self, data: pd.Series) -> pd.DataFrame:

        valid = pd.Series(True, index=data.index)
        error = pd.Series("", index=data.index)

        if self._clip_oor:
            data = data.clip(self._min_val, self._max_val)
        else:
            oor = (data < self._min_val) | (data > self._max_val)
            error[oor] = "Out of range"
            valid[oor] = False

        data = data.round(self._rounding)
        if self._type == "object":
            data = number_to_strings(data, self._prefix, self._suffix)
        try:
            data = data.astype(self._type)
        except:
            print(f"Error casting {data.name} to {self._type}")

        data[~valid] = np.nan

        return pd.concat(
            [data, valid, error],
            axis=1,
            keys=[const.DATA_COL_NAME, const.VALID_COL_NAME, const.ERROR_COL_NAME],
        )
