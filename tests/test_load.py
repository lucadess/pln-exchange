from unittest.mock import MagicMock

import pytest

from src.load import Load


class FakeCatalog:
    """Minimal stand-in for spark.catalog: tracks which fully qualified catalog.schema.table names exist."""

    def __init__(self, existing_tables=None):
        self.existing_tables = set(existing_tables or [])

    def tableExists(self, table_name):
        return table_name in self.existing_tables


@pytest.fixture
def spark():
    return MagicMock()


@pytest.fixture
def loader(spark):
    return Load(spark, catalog_name="test_catalog", schema_name="test_schema", table_name="test_table")


class TestEnsureTableExists:
    def test_creates_empty_table_when_missing(self, loader, spark):
        spark.catalog = FakeCatalog(existing_tables=[])
        df = MagicMock()

        loader.ensure_table_exists(df)

        spark.createDataFrame.assert_called_once_with([], df.schema)
        empty_df = spark.createDataFrame.return_value
        empty_df.write.format.assert_called_once_with("delta")
        empty_df.write.format.return_value.saveAsTable.assert_called_once_with(
            "test_catalog.test_schema.test_table"
        )

    def test_skips_creation_when_table_already_exists(self, loader, spark):
        spark.catalog = FakeCatalog(existing_tables=["test_catalog.test_schema.test_table"])
        df = MagicMock()

        loader.ensure_table_exists(df)

        spark.createDataFrame.assert_not_called()


class TestWrite:
    def test_ensures_table_exists_before_writing(self, loader):
        df = MagicMock()
        loader.ensure_table_exists = MagicMock()

        loader.write(df)

        loader.ensure_table_exists.assert_called_once_with(df)

    def test_writes_df_with_expected_format_mode_and_table(self, loader):
        df = MagicMock()
        loader.ensure_table_exists = MagicMock()

        loader.write(df)

        df.write.format.assert_called_once_with("delta")
        writer = df.write.format.return_value
        writer.mode.assert_called_once_with("overwrite")
        writer.mode.return_value.option.assert_called_once_with("overwriteSchema", "true")
        writer.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            "test_catalog.test_schema.test_table"
        )
