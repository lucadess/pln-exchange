# Databricks notebook source
# MAGIC %md
# MAGIC # PLN Exchange Rate Analysis
# MAGIC Answers the four business questions from the case document, using SQL against the
# MAGIC `pln_exchange_rates` Delta table. Day-over-day swings are measured as **percentage
# MAGIC change**, not absolute change, since EUR/GBP (~4-5 PLN) and JPY (~0.035 PLN) sit on
# MAGIC very different scales - an absolute move means something different for each.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "falck_case")
dbutils.widgets.text("schema_name", "gold")
dbutils.widgets.text("table_name", "pln_exchange_rates")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")

spark.table(f"{catalog_name}.{schema_name}.{table_name}").createOrReplaceTempView("rates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1: Highest depreciation against PLN (lowest historical value per currency)
# MAGIC The date each currency was worth the *fewest* PLN in the whole dataset.

# COMMAND ----------

lowest_value_per_currency = spark.sql("""
    SELECT currency_code, currency_name, effective_date, exchange_rate
    FROM (
        SELECT
            currency_code,
            currency_name,
            effective_date,
            exchange_rate,
            ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY exchange_rate ASC) AS rnk
        FROM rates
    )
    WHERE rnk = 1
    ORDER BY currency_code
""")
display(lowest_value_per_currency)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2: Highest appreciation against PLN (highest historical value per currency)
# MAGIC The date each currency was worth the *most* PLN in the whole dataset.

# COMMAND ----------

highest_value_per_currency = spark.sql("""
    SELECT currency_code, currency_name, effective_date, exchange_rate
    FROM (
        SELECT
            currency_code,
            currency_name,
            effective_date,
            exchange_rate,
            ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY exchange_rate DESC) AS rnk
        FROM rates
    )
    WHERE rnk = 1
    ORDER BY currency_code
""")
display(highest_value_per_currency)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared step for Q3 & Q4: day-over-day change per currency
# MAGIC Both questions rank the same day-over-day percentage change, just in opposite
# MAGIC directions, so it's computed once here and reused below.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMP VIEW daily_change AS
    SELECT
        currency_code,
        currency_name,
        effective_date,
        exchange_rate,
        exchange_rate - LAG(exchange_rate) OVER (PARTITION BY currency_code ORDER BY effective_date) AS rate_change,
        (exchange_rate - LAG(exchange_rate) OVER (PARTITION BY currency_code ORDER BY effective_date))
            / LAG(exchange_rate) OVER (PARTITION BY currency_code ORDER BY effective_date) * 100 AS percentage_change
    FROM rates
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3: Biggest depreciation in a single day (extreme fall)
# MAGIC The date each currency dropped the most, in percentage terms, versus the previous day.

# COMMAND ----------

biggest_one_day_fall = spark.sql("""
    SELECT currency_code, currency_name, effective_date, exchange_rate, rate_change, percentage_change
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY percentage_change ASC) AS rnk
        FROM daily_change
        WHERE percentage_change IS NOT NULL
    )
    WHERE rnk = 1
    ORDER BY currency_code
""")
display(biggest_one_day_fall)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4: Biggest appreciation in a single day (extreme growth)
# MAGIC The date each currency rose the most, in percentage terms, versus the previous day.

# COMMAND ----------

biggest_one_day_rise = spark.sql("""
    SELECT currency_code, currency_name, effective_date, exchange_rate, rate_change, percentage_change
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY percentage_change DESC) AS rnk
        FROM daily_change
        WHERE percentage_change IS NOT NULL
    )
    WHERE rnk = 1
    ORDER BY currency_code
""")
display(biggest_one_day_rise)
