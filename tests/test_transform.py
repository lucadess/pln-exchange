import pytest
import yaml
from pyspark.sql import SparkSession

from src.extract import TABLE_SCHEMA
from src.transform import Transform

RAW_CONFIG = {
    "currencies": [
        {"code": "EUR", "name": "Euro"},
        {"code": "USD", "name": "US Dollar"},
    ],
}


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def config_path(tmp_path):
    """Write a real config.yaml so Transform() exercises its own file-reading path."""
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(RAW_CONFIG))
    return str(path)


@pytest.fixture
def transformer(spark, config_path):
    return Transform(spark, config_path=config_path)


def _raw_df(spark, tables):
    """Build a raw DataFrame in the shape Extract.ingest() produces (see src/extract.py TABLE_SCHEMA)."""
    return spark.createDataFrame(tables, schema=TABLE_SCHEMA)


class TestLoadConfig:
    def test_reads_currencies_from_config(self, transformer):
        assert transformer.config["currencies"] == RAW_CONFIG["currencies"]


class TestExplode:
    def test_one_row_per_rate(self, transformer, spark):
        raw_df = _raw_df(
            spark,
            [
                {
                    "table": "A",
                    "no": "001/A/NBP/2024",
                    "effectiveDate": "2024-01-02",
                    "rates": [
                        {"currency": "euro", "code": "EUR", "mid": 4.35},
                        {"currency": "dolar", "code": "USD", "mid": 3.94},
                    ],
                },
            ],
        )

        result_df = transformer.transform(raw_df)

        assert result_df.count() == 2

    def test_empty_rates_array_produces_no_rows(self, transformer, spark):
        raw_df = _raw_df(
            spark,
            [
                {
                    "table": "A",
                    "no": "001/A/NBP/2024",
                    "effectiveDate": "2024-01-02",
                    "rates": [],
                },
            ],
        )

        result_df = transformer.transform(raw_df)

        assert result_df.count() == 0


class TestFilterCurrencies:
    def test_keeps_only_matching_codes(self, spark):
        df = spark.createDataFrame(
            [
                ("2024-01-02", "euro", "EUR", 4.35),
                ("2024-01-02", "bat", "THB", 0.11),
            ],
            ["effectiveDate", "currency", "code", "mid"],
        )

        result_df = Transform.filter_currencies(df, ["EUR", "USD"])

        assert [row["code"] for row in result_df.collect()] == ["EUR"]

    def test_transform_excludes_non_configured_currencies(self, transformer, spark):
        raw_df = _raw_df(
            spark,
            [
                {
                    "table": "A",
                    "no": "001/A/NBP/2024",
                    "effectiveDate": "2024-01-02",
                    "rates": [
                        {"currency": "euro", "code": "EUR", "mid": 4.35},
                        {"currency": "dolar", "code": "USD", "mid": 3.94},
                        {"currency": "bat", "code": "THB", "mid": 0.11},
                    ],
                },
            ],
        )

        result_df = transformer.transform(raw_df)

        assert {row["currency_code"] for row in result_df.collect()} == {"EUR", "USD"}


class TestRenaming:
    def test_columns_are_renamed(self, transformer, spark):
        raw_df = _raw_df(
            spark,
            [
                {
                    "table": "A",
                    "no": "001/A/NBP/2024",
                    "effectiveDate": "2024-01-02",
                    "rates": [{"currency": "euro", "code": "EUR", "mid": 4.35}],
                },
            ],
        )

        result_df = transformer.transform(raw_df)

        assert set(result_df.columns) == {"effective_date", "currency_name", "currency_code", "exchange_rate"}
