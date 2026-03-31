import pytest

from youtube_scraper import (
    ENGAGEMENT_THRESHOLDS_BY_TIER,
    _score_from_thresholds,
    compute_scores,
    score_croissance,
    score_engagement,
    score_pertinence,
    score_regularite,
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
    "growth,expected",
    [
        (50, 100),
        (30, 100),
        (20, 85),
        (10, 65),
        (5, 45),
        (1, 25),
        (0.5, 10),
        (0, 10),
    ],
)
def test_score_croissance(growth, expected):
    assert score_croissance(growth) == expected


class TestComputeScores:
    def test_with_video_stats(self):
        se, sc, sp, sr, sg = compute_scores(0.10, 5, 3, 20, has_video_stats=True)
        assert se == 100  # engagement
        assert sp == 85  # pertinence
        assert sr == 85  # regularite
        assert sc == 85  # croissance
        assert sg == pytest.approx(100 * 0.28 + 85 * 0.37 + 85 * 0.15 + 85 * 0.20, abs=0.2)

    def test_without_video_stats(self):
        se, sc, sp, sr, sg = compute_scores(0.10, 5, 3, 20, has_video_stats=False)
        assert se == 0  # engagement not computed

    def test_all_scores_in_range(self):
        se, sc, sp, sr, sg = compute_scores(0.08, 3, 2, 10, has_video_stats=True)
        for name, val in [("se", se), ("sc", sc), ("sp", sp), ("sr", sr), ("sg", sg)]:
            assert 0 <= val <= 100, f"{name}={val} out of range"

    def test_high_profile_scores_high(self):
        _, _, _, _, sg = compute_scores(0.10, 10, 4, 30, has_video_stats=True)
        assert sg >= 85

    def test_empty_profile_scores_low(self):
        _, _, _, _, sg = compute_scores(0.0, 0, 0, 0, has_video_stats=True)
        assert sg <= 25

    def test_bug1_regression_cli_uses_has_video_stats(self):
        """Bug 1: compute_scores() was called without has_video_stats in CLI.
        With video stats, engagement should be nonzero for nonzero rates."""
        se_with, _, _, _, sg_with = compute_scores(0.05, 3, 2, 10, has_video_stats=True)
        se_without, _, _, _, sg_without = compute_scores(0.05, 3, 2, 10, has_video_stats=False)
        assert se_with > 0
        assert se_without == 0
        assert sg_with != sg_without

    def test_tier_based_engagement_scoring(self):
        """Macro channels should get higher engagement scores at lower rates."""
        # 3% engagement: macro gets 70 (full score at that tier), mid gets 55 (default thresholds)
        se_macro, _, _, _, _ = compute_scores(0.03, 3, 2, 10, has_video_stats=True, tier="macro")
        se_mid, _, _, _, _ = compute_scores(0.03, 3, 2, 10, has_video_stats=True, tier="mid")
        assert se_macro > se_mid

    def test_tier_none_uses_default_thresholds(self):
        """When tier is None, use default ENGAGEMENT_THRESHOLDS."""
        se_none, _, _, _, _ = compute_scores(0.05, 3, 2, 10, has_video_stats=True, tier=None)
        se_mid, _, _, _, _ = compute_scores(0.05, 3, 2, 10, has_video_stats=True, tier="mid")
        assert se_none == se_mid


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
