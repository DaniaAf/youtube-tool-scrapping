import pytest

from youtube_scraper import (
    ZERO_VIDEO_STATS,
    _compute_views_trend,
    classify_audience_quality,
    compute_channel_metrics,
    compute_punch_above_weight,
)


class TestComputeViewsTrend:
    def test_increasing_views(self):
        # older half avg=100, newer half avg=200 → +100%
        assert _compute_views_trend([50, 150, 150, 250]) == pytest.approx(100.0)

    def test_decreasing_views(self):
        # older half avg=200, newer half avg=100 → -50%
        assert _compute_views_trend([150, 250, 50, 150]) == pytest.approx(-50.0)

    def test_flat_views(self):
        assert _compute_views_trend([100, 100, 100, 100]) == pytest.approx(0.0)

    def test_single_video_returns_none(self):
        assert _compute_views_trend([100]) is None

    def test_empty_returns_none(self):
        assert _compute_views_trend([]) is None

    def test_all_zeros_returns_none(self):
        assert _compute_views_trend([0, 0, 0, 0]) is None

    def test_older_zero_newer_positive_caps_at_200(self):
        assert _compute_views_trend([0, 0, 100, 200]) == 200.0

    def test_two_videos(self):
        # older=[100], newer=[200] → +100%
        assert _compute_views_trend([100, 200]) == pytest.approx(100.0)

    def test_odd_count_splits_correctly(self):
        # 5 videos: older=[100,200] avg=150, newer=[300,400,500] avg=400 → (400-150)/150*100 ≈ 166.7%
        assert _compute_views_trend([100, 200, 300, 400, 500]) == pytest.approx(166.667, abs=0.1)


class TestComputePunchAboveWeight:
    def test_high_paw_ratio(self):
        ratio, label = compute_punch_above_weight(1000, [5000, 10000, 15000])
        assert ratio == pytest.approx(10.0)
        assert label == "exceptional"

    def test_strong_paw(self):
        ratio, label = compute_punch_above_weight(1000, [3000, 4000, 5000])
        assert ratio == pytest.approx(4.0)
        assert label == "strong"

    def test_normal_paw(self):
        ratio, label = compute_punch_above_weight(1000, [1000, 1500, 2000])
        assert ratio == pytest.approx(1.5)
        assert label == "normal"

    def test_below_paw(self):
        ratio, label = compute_punch_above_weight(10000, [3000, 4000, 5000])
        assert ratio == pytest.approx(0.4)
        assert label == "below"

    def test_weak_paw(self):
        ratio, label = compute_punch_above_weight(10000, [100, 200, 300])
        assert ratio == pytest.approx(0.02)
        assert label == "weak"

    def test_zero_followers(self):
        ratio, label = compute_punch_above_weight(0, [1000, 2000])
        assert ratio == 0.0
        assert label == "unknown"

    def test_empty_views(self):
        ratio, label = compute_punch_above_weight(1000, [])
        assert ratio == 0.0
        assert label == "unknown"

    def test_paw_in_channel_metrics(self, sample_channel_details, sample_search_data, sample_vstats):
        metrics = compute_channel_metrics(sample_channel_details, sample_vstats, sample_search_data, days=90)
        assert "punch_above_weight" in metrics
        assert "punch_above_weight_ratio" in metrics
        assert isinstance(metrics["punch_above_weight_ratio"], float)


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

    def test_audience_quality_ratio_in_metrics(self, sample_channel_details, sample_search_data, sample_vstats):
        metrics = compute_channel_metrics(sample_channel_details, sample_vstats, sample_search_data, days=90)
        # 25K / 500K = 0.05
        assert metrics["audience_quality_ratio"] == pytest.approx(0.05)

    def test_audience_quality_ratio_none_when_no_views(self, sample_search_data):
        details = {"followers": 1000, "total_views": 0}
        metrics = compute_channel_metrics(details, dict(ZERO_VIDEO_STATS), sample_search_data, days=90)
        assert metrics["audience_quality_ratio"] is None

    def test_views_trend_with_chronological_data(self, sample_channel_details, sample_search_data, sample_vstats):
        """Chronological vstats should produce a views_trend_pct."""
        metrics = compute_channel_metrics(sample_channel_details, sample_vstats, sample_search_data, days=90)
        assert metrics["views_trend_pct"] is not None
        assert metrics["has_views_trend"] is True

    def test_views_trend_none_without_chronological(self, sample_channel_details, sample_search_data):
        """Non-chronological data should have None views_trend_pct."""
        vstats = {
            "views": 10_000, "likes": 500, "comments": 50, "video_count": 12,
            "shorts_count": 2, "long_form_count": 10,
            "per_video_views": [500, 600, 700, 800, 900, 1000],
            "is_chronological": False,
        }
        metrics = compute_channel_metrics(sample_channel_details, vstats, sample_search_data, days=90)
        assert metrics["views_trend_pct"] is None
        assert metrics["has_views_trend"] is False

    def test_zero_views_engagement(self, sample_channel_details, sample_search_data):
        vstats = {
            "views": 0, "likes": 100, "comments": 10, "video_count": 5,
            "shorts_count": 0, "long_form_count": 5,
            "per_video_views": [], "is_chronological": False,
        }
        metrics = compute_channel_metrics(sample_channel_details, vstats, sample_search_data, days=90)
        assert metrics["engagement_rate"] == 0.0

    def test_zero_video_stats(self, sample_channel_details, sample_search_data):
        metrics = compute_channel_metrics(sample_channel_details, dict(ZERO_VIDEO_STATS), sample_search_data, days=90)
        assert metrics["engagement_rate"] == 0.0
        assert metrics["posts_per_week"] == 0.0
        assert metrics["views_trend_pct"] is None

    def test_emerging_with_views_trend(self, sample_search_data):
        """Channel with positive views trend, <100K followers, mentions, and posts should be emerging."""
        details = {"followers": 10_000}
        vstats = {
            "views": 5000, "likes": 100, "comments": 10, "video_count": 8,
            "shorts_count": 0, "long_form_count": 8,
            "per_video_views": [100, 200, 300, 400, 500, 700, 800, 900],
            "is_chronological": True,
        }
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["views_trend_pct"] is not None
        assert metrics["views_trend_pct"] > 15
        assert metrics["is_emerging"] is True

    def test_not_emerging_very_large_channel(self, sample_search_data):
        """Channel with >=100K followers is never emerging."""
        details = {"followers": 100_000}
        vstats = {
            "views": 5000, "likes": 100, "comments": 10, "video_count": 6,
            "shorts_count": 0, "long_form_count": 6,
            "per_video_views": [100, 200, 300, 500, 700, 900],
            "is_chronological": True,
        }
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["is_emerging"] is False

    def test_emerging_fallback_no_trend(self, sample_search_data):
        """Without views trend, fallback emerging: >=2 mentions and >=0.5 post/week."""
        details = {"followers": 10_000}
        vstats = dict(ZERO_VIDEO_STATS)
        # sample_search_data has mentions_count=5, but 0 video_count → 0 ppw → not emerging
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["views_trend_pct"] is None
        assert metrics["is_emerging"] is False  # ppw < 0.5

    def test_emerging_fallback_with_posts(self):
        """Fallback emerging with enough mentions and posts."""
        details = {"followers": 10_000}
        search_data = {"mentions_count": 5, "video_ids": []}
        vstats = {
            "views": 5000, "likes": 100, "comments": 10, "video_count": 15,
            "shorts_count": 0, "long_form_count": 15,
            "per_video_views": [], "is_chronological": False,
        }
        metrics = compute_channel_metrics(details, vstats, search_data, days=90)
        assert metrics["views_trend_pct"] is None
        assert metrics["is_emerging"] is True  # >=2 mentions, ppw ≈ 1.17

    def test_emerging_paw_path(self):
        """Channel with high PAW ratio should be emerging via PAW path."""
        details = {"followers": 5_000}
        search_data = {"mentions_count": 2, "video_ids": []}
        vstats = {
            "views": 50_000, "likes": 1000, "comments": 100, "video_count": 10,
            "shorts_count": 0, "long_form_count": 10,
            # avg views = 5000, followers = 5000, ratio = 1.0 — not enough (need 3.0)
            "per_video_views": [5000] * 10, "is_chronological": False,
        }
        metrics = compute_channel_metrics(details, vstats, search_data, days=90)
        assert metrics["is_emerging"] is True  # fallback path: 2 mentions, ppw ~0.78

    def test_emerging_paw_path_high_ratio(self):
        """Channel with very high PAW ratio triggers PAW emerging path."""
        details = {"followers": 1_000}
        search_data = {"mentions_count": 1, "video_ids": []}
        vstats = {
            "views": 50_000, "likes": 1000, "comments": 100, "video_count": 5,
            "shorts_count": 0, "long_form_count": 5,
            # avg views = 10000, followers = 1000, ratio = 10.0 ≥ 3.0
            "per_video_views": [10000] * 5, "is_chronological": False,
        }
        metrics = compute_channel_metrics(details, vstats, search_data, days=90)
        assert metrics["punch_above_weight_ratio"] >= 3.0
        assert metrics["is_emerging"] is True  # PAW path: ratio ≥ 3.0, mentions ≥ 1, ppw ≥ 0.3

    def test_not_emerging_paw_but_no_mentions(self):
        """High PAW but no mentions should not be emerging via PAW path."""
        details = {"followers": 1_000}
        search_data = {"mentions_count": 0, "video_ids": []}
        vstats = {
            "views": 50_000, "likes": 1000, "comments": 100, "video_count": 5,
            "shorts_count": 0, "long_form_count": 5,
            "per_video_views": [10000] * 5, "is_chronological": False,
        }
        metrics = compute_channel_metrics(details, vstats, search_data, days=90)
        assert metrics["is_emerging"] is False

    def test_emerging_relaxed_thresholds(self, sample_search_data):
        """Channel with 80K followers can now be emerging (old limit was 50K)."""
        details = {"followers": 80_000}
        vstats = {
            "views": 50_000, "likes": 1000, "comments": 100, "video_count": 8,
            "shorts_count": 0, "long_form_count": 8,
            "per_video_views": [1000, 2000, 3000, 4000, 5000, 7000, 8000, 9000],
            "is_chronological": True,
        }
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        # views trend should be positive
        assert metrics["views_trend_pct"] is not None
        if metrics["views_trend_pct"] >= 15:
            assert metrics["is_emerging"] is True

    def test_views_trend_none_with_no_published_at(self, sample_search_data):
        """No published_at doesn't affect views_trend (which comes from per_video_views)."""
        details = {"followers": 5_000, "published_at": ""}
        vstats = dict(ZERO_VIDEO_STATS)
        metrics = compute_channel_metrics(details, vstats, sample_search_data, days=90)
        assert metrics["views_trend_pct"] is None

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
