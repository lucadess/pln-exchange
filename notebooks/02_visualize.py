# Databricks notebook source
# MAGIC %md
# MAGIC # PLN Exchange Rate Dynamics
# MAGIC Reads the exchange rates Delta table and plots each currency's daily rate against PLN over time.

# COMMAND ----------

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

# COMMAND ----------

dbutils.widgets.text("catalog_name", "falck_case")
dbutils.widgets.text("schema_name", "gold")
dbutils.widgets.text("table_name", "pln_exchange_rates")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

rates_df = spark.table(f"{catalog_name}.{schema_name}.{table_name}").toPandas()

# COMMAND ----------

# Fixed identity color per currency (never reassigned by filtering/sorting).
CURRENCY_COLORS = {
    "EUR": "#2a78d6",  # blue
    "USD": "#eb6834",  # orange
    "GBP": "#1baf7a",  # aqua
    "JPY": "#eda100",  # yellow
}

SURFACE = "#fcfcfb"
PRIMARY_INK = "#0b0b0b"
MUTED_INK = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"

# COMMAND ----------

currency_codes = [code for code in CURRENCY_COLORS if code in set(rates_df["currency_code"])]

fig, axes = plt.subplots(
    len(currency_codes), 1, figsize=(10, 3 * len(currency_codes)), facecolor=SURFACE, sharex=True
)

for ax, code in zip(axes, currency_codes):
    currency_df = rates_df[rates_df["currency_code"] == code].sort_values("effective_date")

    ax.set_facecolor(SURFACE)
    ax.plot(
        currency_df["effective_date"],
        currency_df["exchange_rate"],
        color=CURRENCY_COLORS[code],
        linewidth=2,
        solid_capstyle="round",
    )

    ax.set_title(f"PLN / {code}", loc="left", color=PRIMARY_INK, fontsize=12, fontweight="bold")
    ax.grid(True, color=GRIDLINE, linewidth=1)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(BASELINE)
    ax.tick_params(colors=MUTED_INK)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    last_row = currency_df.iloc[-1]
    ax.annotate(
        f"{last_row['exchange_rate']:.4f}",
        xy=(last_row["effective_date"], last_row["exchange_rate"]),
        xytext=(6, 0),
        textcoords="offset points",
        va="center",
        color=PRIMARY_INK,
        fontsize=9,
    )

axes[-1].set_xlabel("Date", color=MUTED_INK)
fig.suptitle("PLN Exchange Rate Dynamics", color=PRIMARY_INK, fontsize=14, fontweight="bold", y=1.0)
fig.tight_layout()
plt.show()
