from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    substring,
    length,
    concat,
    min as spark_min,
    max as spark_max,
    round as spark_round,
)
from pyspark.sql.types import DoubleType, StringType


class SparkTransformer:
    """
    An efficient, vectorized Spark-based transformer that consolidates conversion and
    normalization logic into a single class. It processes all column transformations
    in a single Spark job for optimal performance.
    """

    def __init__(self, config: dict, spark: SparkSession = None):
        """
        Parameters
        ----------
        config : dict
            A dictionary where keys are column names and values are dictionaries
            containing 'converter' and 'normalizer' configurations for that column.
        spark : SparkSession = None
            An existing SparkSession. If not provided, a new one will be created.
        """
        self.config = config
        self.spark = spark or SparkSession.builder.appName("BDTransformer").getOrCreate()
        self.column_params = {}

    def fit(self, data: DataFrame) -> "SparkTransformer":
        """
        Analyzes the input data to determine the necessary parameters for
        transformation based on the provided configuration. It calculates statistics
        like min and max in a single Spark job.

        Parameters
        ----------
        data : DataFrame
            The input Spark DataFrame to fit the transformer on.

        Returns
        -------
        SparkTransformer
            The fitted transformer instance.
        """
        # 1. Determine column types from a single data sample.
        config_cols = list(self.config.keys())
        sample_data = data.select(*config_cols).limit(1).collect()[0]

        string_cols, numeric_cols, col_types = [], [], {}
        for i, col_name in enumerate(config_cols):
            col_type = type(sample_data[i])
            col_types[col_name] = (
                col_type.__name__ if hasattr(col_type, "__name__") else "unknown"
            )
            if isinstance(sample_data[i], str):
                string_cols.append(col_name)
            else:
                numeric_cols.append(col_name)

        # 2. Build aggregation expressions to find min/max for all columns in one go.
        agg_exprs = []
        for col_name in numeric_cols:
            agg_exprs.extend(
                [
                    spark_min(col(col_name)).alias(f"{col_name}_min"),
                    spark_max(col(col_name)).alias(f"{col_name}_max"),
                ]
            )

        for col_name in string_cols:
            conv_config = self.config[col_name].get("converter", {})
            col_expr = col(col_name)
            if conv_config.get("prefix"):
                prefix_len = len(conv_config["prefix"])
                col_expr = substring(col_expr, prefix_len + 1, length(col_expr))
            if conv_config.get("suffix"):
                suffix_len = len(conv_config["suffix"])
                col_expr = substring(col_expr, 1, length(col_expr) - suffix_len)
            col_expr = col_expr.cast(DoubleType())
            agg_exprs.extend(
                [
                    spark_min(col_expr).alias(f"{col_name}_min"),
                    spark_max(col_expr).alias(f"{col_name}_max"),
                ]
            )

        # 3. Execute the single aggregation job to get all statistics.
        stats_result = {}
        if agg_exprs:
            stats_row = data.agg(*agg_exprs).collect()[0]
            for col_name in config_cols:
                stats_result[col_name] = {
                    "min": stats_row[f"{col_name}_min"],
                    "max": stats_row[f"{col_name}_max"],
                }

        # 4. Populate the final parameter map for each column.
        for col_name, col_config in self.config.items():
            params = {}
            conv_config = col_config.get("converter", {})
            norm_config = col_config.get("normalizer", {})

            params["type"] = col_types[col_name]
            params["prefix"] = conv_config.get("prefix", "")
            params["suffix"] = conv_config.get("suffix", "")
            params["rounding"] = conv_config.get("rounding", 6)
            params["clip_oor"] = conv_config.get("clip_oor", True)
            params["norm_clip"] = norm_config.get("clip", False)

            emp_min = (
                float(stats_result.get(col_name, {}).get("min", 0.0))
                if stats_result.get(col_name, {}).get("min") is not None
                else 0.0
            )
            emp_max = (
                float(stats_result.get(col_name, {}).get("max", 1.0))
                if stats_result.get(col_name, {}).get("max") is not None
                else 1.0
            )

            # Determine min_val for converter clipping
            min_conf = conv_config.get("min_val", False)
            if min_conf is True:
                params["min_val"] = emp_min
            elif isinstance(min_conf, str):
                val = min_conf
                if params["prefix"]:
                    val = val[len(params["prefix"]) :]
                if params["suffix"]:
                    val = val[: -len(params["suffix"])]
                params["min_val"] = float(val)
            elif isinstance(min_conf, (int, float)):
                params["min_val"] = float(min_conf)
            else:
                params["min_val"] = float("-inf")

            # Determine max_val for converter clipping
            max_conf = conv_config.get("max_val", False)
            if max_conf is True:
                params["max_val"] = emp_max
            elif isinstance(max_conf, str):
                val = max_conf
                if params["prefix"]:
                    val = val[len(params["prefix"]) :]
                if params["suffix"]:
                    val = val[: -len(params["suffix"])]
                params["max_val"] = float(val)
            elif isinstance(max_conf, (int, float)):
                params["max_val"] = float(max_conf)
            else:
                params["max_val"] = float("inf")

            # Set normalizer min, max, and scale based on empirical data
            params["norm_min"] = emp_min
            params["norm_max"] = emp_max
            scale = emp_max - emp_min
            params["norm_scale"] = 1.0 if scale == 0 else scale

            self.column_params[col_name] = params

        return self

    def transform(self, data: DataFrame) -> DataFrame:
        """
        Applies the fitted conversion and normalization to the DataFrame.

        Parameters
        ----------
        data : DataFrame
            The input Spark DataFrame to transform.

        Returns
        -------
        DataFrame
            The transformed DataFrame.
        """
        select_exprs = [c for c in data.columns if c not in self.column_params]

        for col_name, params in self.column_params.items():
            col_expr = col(col_name)

            if params["type"] == "str":
                if params["prefix"]:
                    prefix_len = len(params["prefix"])
                    col_expr = substring(col_expr, prefix_len + 1, length(col_expr))
                if params["suffix"]:
                    suffix_len = len(params["suffix"])
                    col_expr = substring(col_expr, 1, length(col_expr) - suffix_len)
                col_expr = col_expr.cast(DoubleType())

            if params["clip_oor"]:
                col_expr = (
                    when(col_expr < params["min_val"], params["min_val"])
                    .when(col_expr > params["max_val"], params["max_val"])
                    .otherwise(col_expr)
                )

            if params["norm_clip"]:
                col_expr = (
                    when(col_expr < params["norm_min"], params["norm_min"])
                    .when(col_expr > params["norm_max"], params["norm_max"])
                    .otherwise(col_expr)
                )

            col_expr = (col_expr - params["norm_min"]) / params["norm_scale"]
            select_exprs.append(col_expr.alias(col_name))

        return data.select(*select_exprs)

    def inverse_transform(self, data: DataFrame) -> DataFrame:
        """
        Applies the inverse transformation to the DataFrame, converting normalized
        data back to its original scale and format.

        Parameters
        ----------
        data : DataFrame
            The transformed Spark DataFrame to inverse transform.

        Returns
        -------
        DataFrame
            The inverse-transformed DataFrame.
        """
        select_exprs = [c for c in data.columns if c not in self.column_params]

        for col_name, params in self.column_params.items():
            col_expr = col(col_name)

            col_expr = (col_expr * params["norm_scale"]) + params["norm_min"]

            if params["norm_clip"]:
                col_expr = (
                    when(col_expr < params["norm_min"], params["norm_min"])
                    .when(col_expr > params["norm_max"], params["norm_max"])
                    .otherwise(col_expr)
                )

            if params["rounding"] is not None:
                col_expr = spark_round(col_expr, params["rounding"])

            if params["type"] == "str":
                col_expr = col_expr.cast(StringType())
                if params["prefix"]:
                    col_expr = concat(lit(params["prefix"]), col_expr)
                if params["suffix"]:
                    col_expr = concat(col_expr, lit(params["suffix"]))
            elif params["type"] in ["int", "int64", "int32"]:
                col_expr = col_expr.cast("long")
            else:
                col_expr = col_expr.cast(DoubleType())

            select_exprs.append(col_expr.alias(col_name))

        return data.select(*select_exprs)