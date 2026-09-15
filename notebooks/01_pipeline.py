# %% [markdown]
# # PLN Exchange Rate Pipeline
# Extract step: pulls raw NBP exchange rate tables into a Spark DataFrame.

# %%
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# %%
from src.extract import Extract

# %%
extractor = Extract(spark, config_path=str(PROJECT_ROOT / "config.yaml"))
raw_df = extractor.ingest()

# %%
raw_df.printSchema()
raw_df.show(5, truncate=100)

# %%
raw_df.count()
