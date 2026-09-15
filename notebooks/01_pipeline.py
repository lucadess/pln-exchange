# Databricks notebook source
# MAGIC %md
# MAGIC # PLN Exchange Rate Pipeline
# MAGIC Extracts NBP exchange rate tables, flattens them, and writes the result to a Delta table.

# COMMAND ----------

from src.extract import Extract
from src.transform import Transform
from src.load import Load

# COMMAND ----------

extractor = Extract(spark, config_path="../config.yaml")
raw_df = extractor.ingest()

# COMMAND ----------

transformer = Transform(spark, config_path="../config.yaml")
flattened_df = transformer.transform(raw_df)

# COMMAND ----------

loader = Load(spark, table_key="pln_exchange_rates", config_path="../config.yaml")
loader.write(flattened_df)
