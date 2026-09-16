from pyspark.sql import DataFrame, SparkSession


class Load:
    """Writes a DataFrame to a Delta table identified by catalog_name.schema_name.table_name."""

    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        mode: str = "overwrite",
    ):
        self.spark = spark
        self.mode = mode
        self.table = f"{catalog_name}.{schema_name}.{table_name}"

    def write(self, df: DataFrame) -> None:
        """Create the table if it doesn't exist yet, then write df into it."""
        self.ensure_table_exists(df)
        df.write.format("delta").mode(self.mode).option("overwriteSchema", "true").saveAsTable(self.table)

    def ensure_table_exists(self, df: DataFrame) -> None:
        """Create an empty Delta table matching df's schema if the table doesn't exist yet."""
        if not self.spark.catalog.tableExists(self.table):
            empty_df = self.spark.createDataFrame([], df.schema)
            empty_df.write.format("delta").saveAsTable(self.table)
