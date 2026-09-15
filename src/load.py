from pyspark.sql import DataFrame, SparkSession

from src.utils import load_config


class Load:
    """Writes a DataFrame to the Delta table defined under `tables.<table_key>` in config.yaml."""

    def __init__(self, spark: SparkSession, table_key: str, mode: str = "overwrite", config_path: str = "config.yaml"):
        self.spark = spark
        self.mode = mode
        self.config = load_config(config_path)
        self.table_config = self.config["tables"][table_key]
        self.table = self.build_table_name()

    def write(self, df: DataFrame) -> None:
        """Create the table if it doesn't exist yet, then write df into it."""
        self.ensure_table_exists(df)
        df.write.format("delta").mode(self.mode).option("overwriteSchema", "true").saveAsTable(self.table)

    def ensure_table_exists(self, df: DataFrame) -> None:
        """Create an empty Delta table matching df's schema if the table doesn't exist yet."""
        if not self.spark.catalog.tableExists(self.table):
            empty_df = self.spark.createDataFrame([], df.schema)
            empty_df.write.format("delta").saveAsTable(self.table)

    def build_table_name(self) -> str:
        """Build the fully qualified table name from the table's catalog and schema in config.yaml."""
        return f"{self.table_config['catalog']}.{self.table_config['catalog_schema']}.{self.table_config['table_name']}"