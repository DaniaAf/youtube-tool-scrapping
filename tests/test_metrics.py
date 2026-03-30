import pytest

from youtube_scraper import ZERO_VIDEO_STATS, compute_channel_metrics


class TestComputeChannelMetrics:
    def test_basic_metrics(self, sample_channel_details, sample_search_data, sample_vstats):
        metrics = compute_channel_metrics(sample_channel_details, sample_vstats, sample_search_data, days=90)

        assert metrics["engagement_rate"] == pytest.approx((500 + 50) / 10_000)
        assert metrics["posts_per_week"] == pytest.approx(12 / (90 / 7), abs=0.01)
        assert metrics["mentions"] == 5
        assert metrics["followers"] == 25_000

    def test_zero_views_engagement(self, sample_channel_details, sample_search_data):
        vstats = {"views": 0, "likes": 100, "comments": 10, "video_count": 5}
        metrics = compute_channel_metrics(sample_channel_details, vstats, sample_search_data, days=90)
        assert metrics["engagement_rate"] == 0.0

    def test_zero_video_stats(self, sample_channel_details, sample_search_data):
        metrics = compute_channel_metrics(sample_channel_details, dict(ZERO_VIDEO_STATS), sample_search_data, days=90)
        assert metrics["engagement_rate"] == 0.0
        assert metrics["posts_per_week"] == 0.0

    def test_emerging_flag(self, sample_search_data):
        """Channel with high growth and <50K followers should be emerging."""
        details = {
            "followers": 10_000,
            "published_at": "2025-12-01T00:00:00Z",  # very recent → high growth
        }
        vstats = dict(ZERO_VIDEO_STATS)
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["is_emerging"] is True

    def test_not_emerging_large_channel(self, sample_search_data):
        """Channel with >50K followers is never emerging."""
        details = {
            "followers": 100_000,
            "published_at": "2025-12-01T00:00:00Z",
        }
        vstats = dict(ZERO_VIDEO_STATS)
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["is_emerging"] is False

    def test_growth_with_no_published_at(self, sample_search_data):
        details = {"followers": 5_000, "published_at": ""}
        vstats = dict(ZERO_VIDEO_STATS)
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["croissance_hebdo"] == 0.0
        assert metrics["growth_rate_pct"] == 0.0

    def test_growth_with_invalid_date(self, sample_search_data):
        details = {"followers": 5_000, "published_at": "not-a-date"}
        vstats = dict(ZERO_VIDEO_STATS)
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["croissance_hebdo"] == 0.0
        assert metrics["growth_rate_pct"] == 0.0
