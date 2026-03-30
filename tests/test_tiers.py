import pytest

from youtube_scraper import calculate_tier


@pytest.mark.parametrize("followers,expected", [
    (0, "nano"),
    (500, "nano"),
    (999, "nano"),
    (1_000, "micro"),
    (5_000, "micro"),
    (9_999, "micro"),
    (10_000, "mid"),
    (50_000, "mid"),
    (99_999, "mid"),
    (100_000, "macro"),
    (500_000, "macro"),
    (999_999, "macro"),
    (1_000_000, "mega"),
    (2_000_000, "mega"),
    (10_000_000, "mega"),
])
def test_calculate_tier(followers, expected):
    assert calculate_tier(followers) == expected
