"""
Extraction module for the NBP (Narodowy Bank Polski) exchange rate API.

`Extract.ingest()` reads config.yaml, splits the configured date range into
chunks the API can serve in one request, pulls the raw exchange rate tables
for each chunk, and returns them as a single Spark DataFrame. Flattening the
nested `rates` array happens downstream in `transform.py`.

API reference: https://api.nbp.pl/en.html#kursyWalut
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import requests
from pyspark.sql import DataFrame, SparkSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils import load_config


class Extract:
    """Reads config.yaml and pulls raw NBP exchange rate tables into a Spark DataFrame."""

    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        self.spark = spark
        self.config = load_config(config_path)
        self.api_config = self.config["api"]
        self.session = self.build_session()

    def ingest(self) -> DataFrame:
        """Fetch every date window in the configured range and return one Spark DataFrame."""
        start_date, end_date = self.get_date_range()
        windows = self.get_date_windows(start_date, end_date)
        responses = [self.fetch_window(window_start, window_end) for window_start, window_end in windows]

        json_rdd = self.spark.sparkContext.parallelize(responses)
        return self.spark.read.option("multiLine", True).json(json_rdd)

    def get_date_range(self) -> tuple[date, date]:
        """Resolve the extraction date range from explicit dates or years_back in the config."""
        date_config = self.config["date_range"]
        start = date_config.get("start_date")
        end = date_config.get("end_date")
        if start and end:
            return self.parse_date(start), self.parse_date(end)

        years_back = date_config.get("years_back")
        if not years_back:
            raise ValueError(
                "config.date_range must define either 'years_back', or both 'start_date' and 'end_date'"
            )
        end_date = date.today()
        start_date = self.subtract_years(end_date, years_back)
        return start_date, end_date

    def get_date_windows(self, start_date: date, end_date: date) -> list[tuple[date, date]]:
        """Split [start_date, end_date] into windows no larger than the API's per-request limit."""
        if start_date > end_date:
            raise ValueError(f"start_date ({start_date}) must not be after end_date ({end_date})")

        max_days = self.api_config["max_days_per_request"]
        step = timedelta(days=max_days - 1)

        windows = []
        window_start = start_date
        while window_start <= end_date:
            window_end = min(window_start + step, end_date)
            windows.append((window_start, window_end))
            window_start = window_end + timedelta(days=1)
        return windows

    def build_url(self, window_start: date, window_end: date) -> str:
        """Build the request URL for one date window."""
        base_url = self.api_config["base_url"]
        return f"{base_url}/{window_start.isoformat()}/{window_end.isoformat()}/"

    def build_session(self) -> requests.Session:
        """Create a requests Session that automatically retries transient server errors."""
        retry = Retry(
            total=self.api_config["max_retries"],
            backoff_factor=self.api_config["retry_backoff_seconds"],
            status_forcelist=[500, 502, 503, 504],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session

    def fetch_window(self, window_start: date, window_end: date) -> str:
        """Call the API for one date window and return the raw JSON response body."""
        url = self.build_url(window_start, window_end)
        response = self.session.get(url, params={"format": "json"}, timeout=self.api_config["timeout_seconds"])
        if response.status_code == 404:
            # No tables published for this exact range (e.g. a window landing entirely
            # on a weekend/holiday) - treat as "no data" rather than an error.
            return "[]"
        response.raise_for_status()
        return response.text

    @staticmethod
    def subtract_years(reference_date: date, years: int) -> date:
        """Subtract `years` from a date, falling back a day for a Feb 29 with no matching leap year."""
        try:
            return reference_date.replace(year=reference_date.year - years)
        except ValueError:
            return reference_date.replace(month=2, day=28, year=reference_date.year - years)

    @staticmethod
    def parse_date(value: str | date) -> date:
        """Parse a 'YYYY-MM-DD' string into a date, passing existing date objects through."""
        if isinstance(value, date):
            return value
        return datetime.strptime(value, "%Y-%m-%d").date()
