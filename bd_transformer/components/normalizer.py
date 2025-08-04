import numpy as np
import pandas as pd

import bd_transformer.consts as const


class Normalizer:
    def __init__(self, clip: bool = False, reject: bool = False):
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

        self._min = None
        self._max = None
        self._scale = None

    def fit(self, data: pd.Series) -> "Normalizer":
        self._min = data.min()
        self._max = data.max()
        self._scale = self._max - self._min
        self._scale = 1 if self._scale == 0 else self._scale
        return self

    def normalize(self, data: pd.Series) -> pd.Series:
        if self._clip:
            data = data.clip(self._min, self._max)
        return (data - self._min) / self._scale

    def inverse_normalize(self, data: pd.Series) -> pd.DataFrame:

        valid = pd.Series(True, index=data.index)
        error = pd.Series("", index=data.index)

        if self._clip:
            data = data.clip(0, 1)
        if self._reject:
            oor = (data < 0) | (data > 1)
            error[oor] = "out of range [0,1]"
            valid[oor] = False

        data = (data * self._scale) + self._min
        data[~valid] = np.nan

        return pd.concat(
            [data, valid, error],
            axis=1,
            keys=[const.DATA_COL_NAME, const.VALID_COL_NAME, const.ERROR_COL_NAME],
        )
