import argparse

import pandas as pd
import yaml

from bd_transformer.transformer import Transformer


def run(config_path: str, data_path: str):
    data = pd.read_parquet(data_path)
    print(data.head())
    with open(config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    transformer = Transformer(config)
    transformer.fit(data)

    transformed = transformer.transform(data)
    print(transformed.head())

    inversed = transformer.inverse_transform(transformed)
    print(inversed.head())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_path", type=str, required=False, default="config.yaml"
    )
    parser.add_argument("--data_path", type=str, required=False, default="data/small/")
    args = parser.parse_args()
    run(args.config_path, args.data_path)
