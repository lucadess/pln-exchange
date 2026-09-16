from datetime import date
from unittest.mock import MagicMock

import pytest
import requests
import yaml
from pyspark.sql import SparkSession

from src.extract import Extract

RAW_CONFIG = {
    "api": {
        "base_url": "https://api.nbp.pl/api/exchangerates/tables/A",
        "max_days_per_request": 93,
        "timeout_seconds": 10,
        "max_retries": 3,
        "retry_backoff_seconds": 0,
    },
    "currencies": [
        {"code": "EUR", "name": "Euro"},
        {"code": "USD", "name": "US Dollar"},
    ],
    "date_range": {"years_back": 5},
}


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def config_path(tmp_path):
    """Write a real config.yaml so Extract() exercises its own file-reading path."""
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(RAW_CONFIG))
    return str(path)


@pytest.fixture
def extractor(config_path, spark):
    return Extract(spark=spark, config_path=config_path)


def _fixed_today(fixed_date):
    """Return a datetime.date subclass whose .today() is pinned, for monkeypatching."""

    class _FixedDate(date):
        @classmethod
        def today(cls):
            return fixed_date

    return _FixedDate


def _response(status_code=200, payload=None):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload if payload is not None else []
    if status_code >= 400 and status_code != 404:
        response.raise_for_status.side_effect = requests.HTTPError(f"{status_code} error")
    else:
        response.raise_for_status.return_value = None
    return response


class TestInit:
    def test_reads_config_from_file(self, extractor):
        assert extractor.api_config == RAW_CONFIG["api"]


class TestIngest:
    def test_returns_one_row_per_table_across_all_windows(self, extractor, monkeypatch):
        monkeypatch.setattr(extractor, "get_date_range", lambda: (date(2024, 1, 1), date(2024, 1, 10)))
        monkeypatch.setattr(
            extractor, "get_date_windows", lambda start, end: [(date(2024, 1, 1), date(2024, 1, 10))]
        )

        tables = [
            {"table": "A", "no": "001/A/NBP/2024", "effectiveDate": "2024-01-02",
             "rates": [{"currency": "euro", "code": "EUR", "mid": 4.35}]},
            {"table": "A", "no": "002/A/NBP/2024", "effectiveDate": "2024-01-03",
             "rates": [{"currency": "euro", "code": "EUR", "mid": 4.36}]},
        ]
        monkeypatch.setattr(extractor, "fetch_window", lambda start, end: tables)

        df = extractor.ingest()

        assert df.count() == 2
        assert set(df.columns) == {"table", "no", "effectiveDate", "rates"}
        effective_dates = {row["effectiveDate"] for row in df.collect()}
        assert effective_dates == {"2024-01-02", "2024-01-03"}

    def test_empty_window_contributes_no_rows(self, extractor, monkeypatch):
        monkeypatch.setattr(extractor, "get_date_range", lambda: (date(2024, 1, 6), date(2024, 1, 7)))
        monkeypatch.setattr(
            extractor, "get_date_windows", lambda start, end: [(date(2024, 1, 6), date(2024, 1, 7))]
        )
        monkeypatch.setattr(extractor, "fetch_window", lambda start, end: [])

        df = extractor.ingest()

        assert df.count() == 0

    def test_fails_the_whole_run_when_one_window_fails(self, extractor, monkeypatch):
        monkeypatch.setattr(extractor, "get_date_range", lambda: (date(2024, 1, 1), date(2024, 1, 20)))
        monkeypatch.setattr(
            extractor,
            "get_date_windows",
            lambda start, end: [
                (date(2024, 1, 1), date(2024, 1, 10)),
                (date(2024, 1, 11), date(2024, 1, 20)),
            ],
        )

        def fetch_window(window_start, window_end):
            if window_start == date(2024, 1, 11):
                raise requests.HTTPError("500 error")
            return [{"table": "A", "no": "001/A/NBP/2024", "effectiveDate": "2024-01-02", "rates": []}]

        monkeypatch.setattr(extractor, "fetch_window", fetch_window)

        with pytest.raises(requests.HTTPError):
            extractor.ingest()


class TestGetDateRange:
    def test_uses_explicit_dates_when_given(self, extractor):
        extractor.config["date_range"] = {"start_date": "2020-01-01", "end_date": "2020-06-01"}
        assert extractor.get_date_range() == (date(2020, 1, 1), date(2020, 6, 1))

    def test_uses_years_back_when_no_explicit_dates(self, extractor, monkeypatch):
        import src.extract as extract_module

        monkeypatch.setattr(extract_module, "date", _fixed_today(date(2024, 6, 15)))
        extractor.config["date_range"] = {"years_back": 5}
        assert extractor.get_date_range() == (date(2019, 6, 15), date(2024, 6, 15))

    def test_handles_leap_day_start(self, extractor, monkeypatch):
        import src.extract as extract_module

        monkeypatch.setattr(extract_module, "date", _fixed_today(date(2024, 2, 29)))
        extractor.config["date_range"] = {"years_back": 5}
        assert extractor.get_date_range() == (date(2019, 2, 28), date(2024, 2, 29))

    def test_raises_when_neither_years_back_nor_explicit_dates(self, extractor):
        extractor.config["date_range"] = {}
        with pytest.raises(ValueError):
            extractor.get_date_range()


class TestGetDateWindows:
    def test_single_window_when_range_fits(self, extractor):
        windows = extractor.get_date_windows(date(2024, 1, 1), date(2024, 1, 10))
        assert windows == [(date(2024, 1, 1), date(2024, 1, 10))]

    def test_splits_into_multiple_windows(self, extractor):
        extractor.api_config["max_days_per_request"] = 4
        windows = extractor.get_date_windows(date(2024, 1, 1), date(2024, 1, 10))
        assert windows == [
            (date(2024, 1, 1), date(2024, 1, 4)),
            (date(2024, 1, 5), date(2024, 1, 8)),
            (date(2024, 1, 9), date(2024, 1, 10)),
        ]

    def test_raises_when_start_after_end(self, extractor):
        with pytest.raises(ValueError):
            extractor.get_date_windows(date(2024, 1, 10), date(2024, 1, 1))


class TestBuildUrl:
    def test_builds_expected_url(self, extractor):
        url = extractor.build_url(date(2024, 1, 1), date(2024, 1, 10))
        assert url == "https://api.nbp.pl/api/exchangerates/tables/A/2024-01-01/2024-01-10/"


class TestFetchWindow:
    def test_returns_parsed_tables_on_success(self, extractor, monkeypatch):
        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _response(200, [{"table": "A"}]))

        tables = extractor.fetch_window(date(2024, 1, 1), date(2024, 1, 10))
        assert tables == [{"table": "A"}]

    def test_treats_404_as_empty_list(self, extractor, monkeypatch):
        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _response(404))

        assert extractor.fetch_window(date(2024, 1, 6), date(2024, 1, 7)) == []

    def test_raises_on_http_error(self, extractor, monkeypatch):
        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _response(500))

        with pytest.raises(requests.HTTPError):
            extractor.fetch_window(date(2024, 1, 1), date(2024, 1, 10))

    def test_retries_on_transient_error_then_succeeds(self, extractor, monkeypatch):
        responses = iter([requests.ConnectionError("boom"), _response(200, [{"table": "A"}])])

        def fake_get(*args, **kwargs):
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response

        monkeypatch.setattr(requests, "get", fake_get)

        tables = extractor.fetch_window(date(2024, 1, 1), date(2024, 1, 10))
        assert tables == [{"table": "A"}]

    def test_raises_after_exhausting_all_retries(self, extractor, monkeypatch):
        call_count = 0

        def fake_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise requests.ConnectionError("boom")

        monkeypatch.setattr(requests, "get", fake_get)

        with pytest.raises(requests.ConnectionError):
            extractor.fetch_window(date(2024, 1, 1), date(2024, 1, 10))
        assert call_count == extractor.api_config["max_retries"]
