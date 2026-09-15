# %% [markdown]
# # PLN Exchange Rate Pipeline
# Extract step: pulls raw NBP exchange rate tables into a Spark DataFrame.

# %%
from src.extract import Extract

# %%
extractor = Extract(spark, config_path="../config.yaml")
raw_df = extractor.ingest()

# %%
raw_df.printSchema()
raw_df.show(5, truncate=100)

# %%
raw_df.count()
