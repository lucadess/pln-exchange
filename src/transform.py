from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils import load_config


class Transform:
    """Flattens raw NBP exchange rate tables into one row per currency per day."""

    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        self.spark = spark
        self.config = load_config(config_path)

    def transform(self, raw_df: DataFrame) -> DataFrame:
        """Explode the nested 'rates' array, normalize column names, and keep only configured currencies."""
        currency_codes = [currency["code"] for currency in self.config["currencies"]]
        flat_df = raw_df.select(
            "table",
            "no",
            "effectiveDate",
            F.explode("rates").alias("rate"),
        ).select(
            F.to_date("effectiveDate").alias("effectiveDate"),
            "rate.currency",
            "rate.code",
            "rate.mid",
        )

        filtered_df = self.filter_currencies(flat_df, currency_codes).withColumnRenamed(
                "mid", "exchange_rate"
            ).withColumnRenamed(
                "effectiveDate", "effective_date"
            ).withColumnRenamed(
                "currency", "currency_name"
            ).withColumnRenamed(
                "code", "currency_code"
            )
        return filtered_df

    @staticmethod
    def filter_currencies(df: DataFrame, currency_codes: list[str]) -> DataFrame:
        """Keep only rows whose currency code is in currency_codes."""
        return df.filter(F.col("code").isin(currency_codes))