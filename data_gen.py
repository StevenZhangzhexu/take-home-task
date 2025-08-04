import argparse
import os

import numpy as np
import pandas as pd


def generate_data(file_path="data/small/", num_rows=10000, chunk_size=10000):

    os.makedirs(file_path, exist_ok=True)

    for i in range(num_rows // chunk_size):
        df = pd.DataFrame(
            {
                "day_of_month": np.random.randint(1, 31, size=chunk_size),
                "height": [
                    str(round(np.random.rand() * 100, 2)) + "cm"
                    for _ in range(chunk_size)
                ],
                "account_balance": [
                    "$" + str(round(np.random.rand() * 10000, 2))
                    for _ in range(chunk_size)
                ],
                "net_profit": [
                    "$" + str(round((np.random.rand() - 0.5) * 10000, 2))
                    for _ in range(chunk_size)
                ],
                "customer_ratings": [
                    str(round(np.random.rand() * 5, 2)) + "stars"
                    for _ in range(chunk_size)
                ],
                "leaderboard_rank": np.random.randint(1, 100000, size=chunk_size),
            }
        )
        df.to_parquet(f"{file_path}/{(i + 1):03d}.parquet", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", type=str, default="data/small/")
    parser.add_argument("--num_rows", type=int, default=10000)
    parser.add_argument("--chunk_size", type=int, default=10000)
    args = parser.parse_args()
    generate_data(args.file_path, args.num_rows, args.chunk_size)
