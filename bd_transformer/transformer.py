import pandas as pd

import bd_transformer.consts as const
from bd_transformer.components.converter import Converter
from bd_transformer.components.normalizer import Normalizer


class Transformer:
    """
    A transformer that converts and normalizes data according to a given configuration.
    """

    def __init__(self, config: dict):
        self.config = config
        self.converters = {}
        self.normalizers = {}

    def fit(self, data: pd.DataFrame) -> "Transformer":
        """
        Fit the transformer parameters to the data.
        """
        for col in self.config:
            self.converters[col] = Converter(
                **self.config[col].get("converter", {})
            ).fit(data[col])
            converted = self.converters[col].convert(data[col])
            self.normalizers[col] = Normalizer(
                **self.config[col].get("normalizer", {})
            ).fit(converted)
        return self

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the data according to the configuration and fitted parameters.
        """
        for col in self.config:
            converted = self.converters[col].convert(data[col])
            normalized = self.normalizers[col].normalize(converted)
            data[col] = normalized
        return data

    def inverse_transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Inverse transform the data according to the configuration and fitted parameters.
        """
        for col in self.config:
            inverse_normalized = self.normalizers[col].inverse_normalize(data[col])
            valid_mask = inverse_normalized[const.VALID_COL_NAME]
            inverse_converted = self.converters[col].inverse_convert(
                inverse_normalized[const.DATA_COL_NAME][valid_mask]
            )

            inversed = inverse_normalized
            try:
                inversed[const.DATA_COL_NAME] = inversed[const.DATA_COL_NAME].astype(
                    inverse_converted[const.DATA_COL_NAME].dtype
                )
            except:
                print(
                    f"Error casting {col} to {inverse_converted[const.DATA_COL_NAME].dtype}"
                )
            inversed.loc[valid_mask, const.DATA_COL_NAME] = inverse_converted[
                const.DATA_COL_NAME
            ]
            inversed.loc[valid_mask, const.VALID_COL_NAME] = inverse_converted[
                const.VALID_COL_NAME
            ]
            inversed.loc[valid_mask, const.ERROR_COL_NAME] = inverse_converted[
                const.ERROR_COL_NAME
            ]

            data[col] = inversed[const.DATA_COL_NAME]
        return data
