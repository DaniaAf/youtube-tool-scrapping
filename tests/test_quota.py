"""Tests for quota tracking and estimation."""

import math
import re
from unittest.mock import MagicMock, patch

import pytest

from youtube_scraper import (
    QUOTA_COST_LIST,
    QUOTA_COST_SEARCH,
    YOUTUBE_DAILY_QUOTA,
    _execute_api_request,
    _quota_cache_key,
    _quota_ttl_seconds,
    get_quota_used,
    record_quota_usage,
    reset_quota,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestQuotaConstants:
    def test_daily_quota(self):
        assert YOUTUBE_DAILY_QUOTA == 10_000

    def test_search_cost(self):
        assert QUOTA_COST_SEARCH == 100

    def test_list_cost(self):
        assert QUOTA_COST_LIST == 1


# ---------------------------------------------------------------------------
# _quota_cache_key
# ---------------------------------------------------------------------------


class TestQuotaCacheKey:
    def test_format(self):
        key = _quota_cache_key()
        assert key.startswith("quota_used:")
        # Date part should be YYYY-MM-DD
        date_part = key.split(":", 1)[1]
        assert re.match(r"\d{4}-\d{2}-\d{2}$", date_part)

    def test_uses_pacific_time(self):
        """Key should use Pacific time, not UTC."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        pacific_date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")
        key = _quota_cache_key()
        assert key == f"quota_used:{pacific_date}"


# ---------------------------------------------------------------------------
# _quota_ttl_seconds
# ---------------------------------------------------------------------------


class TestQuotaTtlSeconds:
    def test_bounds(self):
        ttl = _quota_ttl_seconds()
        assert 1 <= ttl <= 86400

    def test_returns_int(self):
        assert isinstance(_quota_ttl_seconds(), int)


# ---------------------------------------------------------------------------
# record_quota_usage / get_quota_used / reset_quota
# ---------------------------------------------------------------------------


class TestQuotaTracking:
    @pytest.fixture(autouse=True)
    def _use_temp_cache(self, tmp_path):
        """Override the disk cache with a temporary directory."""
        import diskcache

        temp_cache = diskcache.Cache(str(tmp_path / "test_cache"))
        with patch("youtube_scraper.get_cache", return_value=temp_cache):
            yield temp_cache
        temp_cache.close()

    def test_initial_value_is_zero(self):
        assert get_quota_used() == 0

    def test_record_accumulates(self):
        record_quota_usage(100)
        assert get_quota_used() == 100
        record_quota_usage(50)
        assert get_quota_used() == 150

    def test_reset_clears(self):
        record_quota_usage(200)
        assert get_quota_used() == 200
        reset_quota()
        assert get_quota_used() == 0

    def test_record_single_unit(self):
        record_quota_usage(1)
        assert get_quota_used() == 1


# ---------------------------------------------------------------------------
# _execute_api_request quota recording
# ---------------------------------------------------------------------------


class TestExecuteApiRequestQuota:
    @pytest.fixture(autouse=True)
    def _use_temp_cache(self, tmp_path):
        import diskcache

        temp_cache = diskcache.Cache(str(tmp_path / "test_cache"))
        with patch("youtube_scraper.get_cache", return_value=temp_cache):
            yield temp_cache
        temp_cache.close()

    def test_records_quota_on_success(self):
        mock_request = MagicMock()
        mock_request.execute.return_value = {"items": []}

        _execute_api_request(mock_request, quota_cost=100)
        assert get_quota_used() == 100

    def test_records_correct_cost(self):
        mock_request = MagicMock()
        mock_request.execute.return_value = {"items": []}

        _execute_api_request(mock_request, quota_cost=1)
        _execute_api_request(mock_request, quota_cost=100)
        assert get_quota_used() == 101

    def test_no_quota_on_failure(self):
        from googleapiclient.errors import HttpError

        mock_request = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 400
        mock_request.execute.side_effect = HttpError(mock_resp, b"Bad request")

        with pytest.raises(HttpError):
            _execute_api_request(mock_request, quota_cost=100)
        assert get_quota_used() == 0

    def test_default_cost_is_one(self):
        mock_request = MagicMock()
        mock_request.execute.return_value = {"items": []}

        _execute_api_request(mock_request)
        assert get_quota_used() == 1


# ---------------------------------------------------------------------------
# Estimation formula (mirrors app.py logic)
# ---------------------------------------------------------------------------


def _estimate_quota(kw_count: int, max_channels: int, stats_mode: str) -> int:
    """Reproduce the estimation formula from app.py for testing."""
    pages_per_kw = math.ceil(max_channels / 30)
    search_cost = kw_count * pages_per_kw * QUOTA_COST_SEARCH
    channel_cost = math.ceil(max_channels / 50) * QUOTA_COST_LIST
    if stats_mode == "Full":
        video_cost = max_channels * (QUOTA_COST_SEARCH + QUOTA_COST_LIST)
    elif stats_mode == "Fast":
        video_cost = math.ceil(max_channels * 5 / 50) * QUOTA_COST_LIST
    else:
        video_cost = 0
    return search_cost + channel_cost + video_cost


class TestQuotaEstimation:
    @pytest.mark.parametrize(
        "kw_count,max_channels,mode,expected",
        [
            # 1 kw, 100 ch, Fast: 4*100 + 2 + 10 = 412
            (1, 100, "Fast", 412),
            # 1 kw, 100 ch, None: 4*100 + 2 + 0 = 402
            (1, 100, "None", 402),
            # 1 kw, 100 ch, Full: 4*100 + 2 + 100*101 = 10502
            (1, 100, "Full", 10502),
            # 2 kw, 50 ch, Fast: 2*2*100 + 1 + 5 = 406
            (2, 50, "Fast", 406),
            # 1 kw, 10 ch, None: 1*1*100 + 1 + 0 = 101
            (1, 10, "None", 101),
        ],
    )
    def test_estimation(self, kw_count, max_channels, mode, expected):
        assert _estimate_quota(kw_count, max_channels, mode) == expected

    def test_reasonable_fast_mode(self):
        """1 kw / 100 ch / Fast should be well under 10K quota."""
        est = _estimate_quota(1, 100, "Fast")
        assert est < 1000

    def test_full_mode_expensive(self):
        """Full mode with many channels should be expensive."""
        est = _estimate_quota(1, 100, "Full")
        assert est > 5000
