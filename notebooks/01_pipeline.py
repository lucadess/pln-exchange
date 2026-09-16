# Databricks notebook source
# MAGIC %md
# MAGIC # PLN Exchange Rate Pipeline
# MAGIC Extracts NBP exchange rate tables, flattens them, and writes the result to a Delta table.

# COMMAND ----------

from src.extract import Extract
from src.transform import Transform
from src.load import Load

# COMMAND ----------

dbutils.widgets.text("catalog_name", "falck_case")
dbutils.widgets.text("schema_name", "gold")
dbutils.widgets.text("table_name", "pln_exchange_rates")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")

config_path = "../config.yaml"

# COMMAND ----------

extractor = Extract(spark, config_path=config_path)
raw_df = extractor.ingest()

# COMMAND ----------

transformer = Transform(spark, config_path=config_path)
flattened_df = transformer.transform(raw_df)

# COMMAND ----------

loader = Load(spark, catalog_name=catalog_name, schema_name=schema_name, table_name=table_name)
loader.write(flattened_df)
