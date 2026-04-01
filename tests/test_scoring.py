import pytest

from youtube_scraper import (
    ENGAGEMENT_THRESHOLDS_BY_TIER,
    _score_from_thresholds,
    compute_scores,
    score_audience_quality,
    score_croissance,
    score_engagement,
    score_pertinence,
    score_regularite,
    score_shorts_content,
)


class TestScoreFromThresholds:
    def test_returns_first_matching_score(self):
        thresholds = [(10, 100), (5, 50), (0, 10)]
        assert _score_from_thresholds(15, thresholds) == 100
        assert _score_from_thresholds(10, thresholds) == 100
        assert _score_from_thresholds(7, thresholds) == 50
        assert _score_from_thresholds(5, thresholds) == 50
        assert _score_from_thresholds(3, thresholds) == 10

    def test_returns_default_when_below_all(self):
        thresholds = [(10, 100), (5, 50)]
        assert _score_from_thresholds(1, thresholds, default=0) == 0
        assert _score_from_thresholds(-1, thresholds, default=42) == 42

    def test_empty_thresholds(self):
        assert _score_from_thresholds(100, [], default=7) == 7


@pytest.mark.parametrize(
    "rate,expected",
    [
        (0.15, 100),
        (0.10, 100),
        (0.07, 85),
        (0.05, 70),
        (0.03, 55),
        (0.01, 35),
        (0.005, 15),
        (0.0, 15),
    ],
)
def test_score_engagement(rate, expected):
    assert score_engagement(rate) == expected


@pytest.mark.parametrize(
    "mentions,expected",
    [
        (15, 100),
        (10, 100),
        (5, 85),
        (3, 65),
        (2, 45),
        (1, 25),
        (0, 0),
    ],
)
def test_score_pertinence(mentions, expected):
    assert score_pertinence(mentions) == expected


@pytest.mark.parametrize(
    "ppw,expected",
    [
        (5, 100),
        (4, 100),
        (3, 85),
        (2, 70),
        (1, 50),
        (0.5, 30),
        (0.2, 10),
        (0, 10),
    ],
)
def test_score_regularite(ppw, expected):
    assert score_regularite(ppw) == expected


@pytest.mark.parametrize(
    "trend,expected",
    [
        (150, 100),
        (100, 100),
        (50, 85),
        (20, 70),
        (5, 55),
        (0, 40),
        (-20, 25),
        (-50, 15),
        (-80, 10),
    ],
)
def test_score_croissance(trend, expected):
    assert score_croissance(trend) == expected


class TestScoreAudienceQuality:
    @pytest.mark.parametrize(
        "ratio,expected",
        [
            (0.15, 100),
            (0.10, 100),
            (0.05, 85),
            (0.03, 70),
            (0.01, 50),
            (0.005, 35),
            (0.001, 20),
            (0.0005, 10),
        ],
    )
    def test_thresholds(self, ratio, expected):
        assert score_audience_quality(ratio) == expected

    def test_zero_ratio(self):
        assert score_audience_quality(0.0) == 10


class TestScoreShortsContent:
    @pytest.mark.parametrize(
        "shorts_ratio,expected",
        [
            (0.0, 100),   # 100% long-form → 1.0 - 0.0 = 1.0 → 100
            (0.1, 100),   # 90% long-form → 1.0 - 0.1 = 0.9 → 100
            (0.3, 90),    # 70% long-form → 1.0 - 0.3 = 0.7 → 90
            (0.5, 70),    # 50% long-form → 1.0 - 0.5 = 0.5 → 70
            (0.7, 50),    # 30% long-form → 1.0 - 0.7 = 0.3 → 50
            (0.9, 15),    # 10% long-form → 1.0 - 0.9 = ~0.0999 (float) → 15
            (1.0, 15),    # 0% long-form → 1.0 - 1.0 = 0.0 → 15
        ],
    )
    def test_thresholds(self, shorts_ratio, expected):
        assert score_shorts_content(shorts_ratio) == expected


class TestComputeScores:
    def test_full_mode_all_six(self):
        """Full mode: all 6 scores active."""
        se, sc, sp, sr, saq, ssc, sg = compute_scores(
            0.10, 5, 3, views_trend_pct=50.0, has_video_stats=True, has_views_trend=True,
            audience_quality_ratio=0.05, shorts_ratio=0.1,
        )
        assert se == 100  # engagement
        assert sp == 85  # pertinence
        assert sr == 85  # regularite
        assert sc == 85  # croissance (50% trend → 85)
        assert saq == 85  # audience quality (0.05 → 85)
        assert ssc == 100  # shorts content (0.1 shorts → 0.9 long → 100)
        expected = (
            85 * 0.25 + 100 * 0.22 + 85 * 0.18 + 85 * 0.12 + 85 * 0.13 + 100 * 0.10
        )
        assert sg == pytest.approx(expected, abs=0.2)

    def test_fast_mode_no_trend(self):
        """Fast mode: engagement available but no views trend."""
        se, sc, sp, sr, saq, ssc, sg = compute_scores(
            0.10, 5, 3, views_trend_pct=None, has_video_stats=True, has_views_trend=False,
            audience_quality_ratio=0.05, shorts_ratio=0.1,
        )
        assert se == 100  # engagement
        assert sc == 0  # no trend
        assert sp == 85
        assert sr == 85
        assert saq == 85
        assert ssc == 100
        # Weights renormalized over active: pertinence+engagement+regularite+aq+shorts (0.25+0.22+0.12+0.13+0.10=0.82)
        active_total = 0.25 + 0.22 + 0.12 + 0.13 + 0.10
        expected = (
            85 * (0.25 / active_total)
            + 100 * (0.22 / active_total)
            + 85 * (0.12 / active_total)
            + 85 * (0.13 / active_total)
            + 100 * (0.10 / active_total)
        )
        assert sg == pytest.approx(expected, abs=0.2)

    def test_none_mode_no_stats(self):
        """None mode: no video stats, no trend."""
        se, sc, sp, sr, saq, ssc, sg = compute_scores(
            0.0, 5, 3, views_trend_pct=None, has_video_stats=False, has_views_trend=False,
            audience_quality_ratio=0.05,
        )
        assert se == 0
        assert sc == 0
        assert ssc == 0  # shorts_content not active without video stats
        assert saq == 85  # audience quality still active
        # Weights renormalized over pertinence+regularite+aq (0.25+0.12+0.13=0.50)
        active_total = 0.25 + 0.12 + 0.13
        expected = (
            85 * (0.25 / active_total) + 85 * (0.12 / active_total) + 85 * (0.13 / active_total)
        )
        assert sg == pytest.approx(expected, abs=0.2)

    def test_none_mode_no_aq(self):
        """None mode without audience quality ratio either."""
        se, sc, sp, sr, saq, ssc, sg = compute_scores(
            0.0, 5, 3, views_trend_pct=None, has_video_stats=False, has_views_trend=False,
        )
        assert saq == 0
        assert ssc == 0
        # Only pertinence+regularite active (0.25+0.12=0.37)
        active_total = 0.25 + 0.12
        expected = 85 * (0.25 / active_total) + 85 * (0.12 / active_total)
        assert sg == pytest.approx(expected, abs=0.2)

    def test_all_scores_in_range(self):
        se, sc, sp, sr, saq, ssc, sg = compute_scores(
            0.08, 3, 2, views_trend_pct=20.0, has_video_stats=True, has_views_trend=True,
            audience_quality_ratio=0.03, shorts_ratio=0.3,
        )
        for name, val in [("se", se), ("sc", sc), ("sp", sp), ("sr", sr), ("saq", saq), ("ssc", ssc), ("sg", sg)]:
            assert 0 <= val <= 100, f"{name}={val} out of range"

    def test_high_profile_scores_high(self):
        *_, sg = compute_scores(
            0.10, 10, 4, views_trend_pct=100.0, has_video_stats=True, has_views_trend=True,
            audience_quality_ratio=0.10, shorts_ratio=0.0,
        )
        assert sg >= 85

    def test_empty_profile_scores_low(self):
        *_, sg = compute_scores(
            0.0, 0, 0, views_trend_pct=-80.0, has_video_stats=True, has_views_trend=True,
            audience_quality_ratio=0.0, shorts_ratio=1.0,
        )
        assert sg <= 25

    def test_bug1_regression_cli_uses_has_video_stats(self):
        """With video stats, engagement should be nonzero for nonzero rates."""
        se_with, _, _, _, _, _, sg_with = compute_scores(
            0.05, 3, 2, views_trend_pct=20.0, has_video_stats=True, has_views_trend=True,
            audience_quality_ratio=0.03, shorts_ratio=0.2,
        )
        se_without, _, _, _, _, _, sg_without = compute_scores(
            0.05, 3, 2, views_trend_pct=None, has_video_stats=False, has_views_trend=False,
            audience_quality_ratio=0.03,
        )
        assert se_with > 0
        assert se_without == 0
        assert sg_with != sg_without

    def test_tier_based_engagement_scoring(self):
        """Macro channels should get higher engagement scores at lower rates."""
        se_macro, _, _, _, _, _, _ = compute_scores(
            0.03, 3, 2, views_trend_pct=20.0, has_video_stats=True, has_views_trend=True, tier="macro",
        )
        se_mid, _, _, _, _, _, _ = compute_scores(
            0.03, 3, 2, views_trend_pct=20.0, has_video_stats=True, has_views_trend=True, tier="mid",
        )
        assert se_macro > se_mid

    def test_tier_none_uses_default_thresholds(self):
        """When tier is None, use default ENGAGEMENT_THRESHOLDS."""
        se_none, _, _, _, _, _, _ = compute_scores(
            0.05, 3, 2, views_trend_pct=20.0, has_video_stats=True, has_views_trend=True, tier=None,
        )
        se_mid, _, _, _, _, _, _ = compute_scores(
            0.05, 3, 2, views_trend_pct=20.0, has_video_stats=True, has_views_trend=True, tier="mid",
        )
        assert se_none == se_mid

    def test_returns_seven_values(self):
        """compute_scores returns a 7-tuple."""
        result = compute_scores(0.05, 3, 2, has_video_stats=True)
        assert len(result) == 7

    def test_audience_quality_excluded_when_none(self):
        """When audience_quality_ratio is None, saq should be 0."""
        _, _, _, _, saq, _, _ = compute_scores(
            0.05, 3, 2, has_video_stats=True, audience_quality_ratio=None,
        )
        assert saq == 0

    def test_shorts_excluded_without_video_stats(self):
        """Shorts content score excluded when has_video_stats=False."""
        _, _, _, _, _, ssc, _ = compute_scores(
            0.05, 3, 2, has_video_stats=False, shorts_ratio=0.5,
        )
        assert ssc == 0


class TestTierEngagementThresholds:
    def test_macro_easier_than_nano(self):
        """Macro channels need lower engagement to score well."""
        se_macro = score_engagement(0.03, tier="macro")
        se_nano = score_engagement(0.03, tier="nano")
        assert se_macro > se_nano

    def test_mega_easiest_thresholds(self):
        se_mega = score_engagement(0.02, tier="mega")
        assert se_mega >= 70

    def test_all_tiers_have_thresholds(self):
        for tier in ["nano", "micro", "mid", "macro", "mega"]:
            assert tier in ENGAGEMENT_THRESHOLDS_BY_TIER
            assert len(ENGAGEMENT_THRESHOLDS_BY_TIER[tier]) >= 4
