import pytest

from youtube_scraper import ZERO_VIDEO_STATS, classify_audience_quality, compute_channel_metrics


class TestComputeChannelMetrics:
    def test_basic_metrics(self, sample_channel_details, sample_search_data, sample_vstats):
        metrics = compute_channel_metrics(sample_channel_details, sample_vstats, sample_search_data, days=90)

        assert metrics["engagement_rate"] == pytest.approx((500 + 50) / 10_000)
        assert metrics["posts_per_week"] == pytest.approx(12 / (90 / 7), abs=0.01)
        assert metrics["mentions"] == 5
        assert metrics["followers"] == 25_000
        assert metrics["total_recent_views"] == 10_000
        assert metrics["total_recent_likes"] == 500
        assert metrics["total_recent_comments"] == 50
        assert metrics["recent_video_count"] == 12
        assert metrics["shorts_count"] == 2
        assert metrics["long_form_count"] == 10
        assert metrics["shorts_ratio"] == pytest.approx(2 / 12, abs=0.001)

    def test_zero_views_engagement(self, sample_channel_details, sample_search_data):
        vstats = {"views": 0, "likes": 100, "comments": 10, "video_count": 5, "shorts_count": 0, "long_form_count": 5}
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

    def test_audience_quality_in_metrics(self, sample_channel_details, sample_search_data, sample_vstats):
        metrics = compute_channel_metrics(sample_channel_details, sample_vstats, sample_search_data, days=90)
        # 25K followers / 500K views = 0.05 → "good"
        assert metrics["audience_quality"] == "good"


class TestClassifyAudienceQuality:
    def test_excellent_ratio(self):
        # 100K subs / 500K views = 0.2 → excellent
        assert classify_audience_quality(100_000, 500_000) == "excellent"

    def test_good_ratio(self):
        # 25K subs / 500K views = 0.05 → good
        assert classify_audience_quality(25_000, 500_000) == "good"

    def test_average_ratio(self):
        # 5K subs / 500K views = 0.01 → average
        assert classify_audience_quality(5_000, 500_000) == "average"

    def test_low_ratio(self):
        # 1K subs / 500K views = 0.002 → low
        assert classify_audience_quality(1_000, 500_000) == "low"

    def test_zero_views_returns_unknown(self):
        assert classify_audience_quality(10_000, 0) == "unknown"

    def test_boundary_excellent(self):
        # Exactly 0.10 ratio → excellent
        assert classify_audience_quality(10_000, 100_000) == "excellent"

    def test_boundary_good(self):
        # Exactly 0.03 → good
        assert classify_audience_quality(3_000, 100_000) == "good"
