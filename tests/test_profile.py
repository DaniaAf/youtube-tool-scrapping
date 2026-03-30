
from youtube_scraper import COLUMNS, ZERO_VIDEO_STATS, build_channel_profile, compute_channel_metrics


class TestBuildChannelProfile:
    def _make_profile(self, details, search_data, vstats, has_video_stats=True, days=90):
        metrics = compute_channel_metrics(details, vstats, search_data, days)
        return build_channel_profile(
            "UC_TEST_123", details, search_data, metrics, has_video_stats, "2025-01-01 12:00:00",
        )

    def test_all_columns_present(self, sample_channel_details, sample_search_data, sample_vstats):
        profile = self._make_profile(sample_channel_details, sample_search_data, sample_vstats)
        for col in COLUMNS:
            assert col in profile, f"Missing column: {col}"

    def test_profile_url_with_username(self, sample_channel_details, sample_search_data, sample_vstats):
        profile = self._make_profile(sample_channel_details, sample_search_data, sample_vstats)
        assert profile["profile_url"] == "https://www.youtube.com/@testcreator"

    def test_profile_url_without_username(self, sample_search_data, sample_vstats):
        details = {"followers": 1000}
        profile = self._make_profile(details, sample_search_data, sample_vstats)
        assert profile["profile_url"] == "https://www.youtube.com/channel/UC_TEST_123"

    def test_bug2_regression_status_without_video_stats(self, sample_channel_details, sample_search_data):
        """Bug 2: status was always 'inactive' when fetch_video_stats=False
        because posts_per_week=0 with zero video stats. Now it should be 'active'."""
        profile = self._make_profile(
            sample_channel_details, sample_search_data, dict(ZERO_VIDEO_STATS), has_video_stats=False,
        )
        assert profile["status"] == "active"

    def test_status_active_with_video_stats(self, sample_channel_details, sample_search_data, sample_vstats):
        profile = self._make_profile(sample_channel_details, sample_search_data, sample_vstats)
        assert profile["posts_per_week"] > 0.5
        assert profile["status"] == "active"

    def test_status_inactive_with_zero_posting(self, sample_channel_details, sample_search_data):
        vstats = {"views": 100, "likes": 10, "comments": 1, "video_count": 0}
        profile = self._make_profile(sample_channel_details, sample_search_data, vstats, has_video_stats=True)
        assert profile["status"] == "inactive"

    def test_bug3_regression_email_field_present(self, sample_channel_details, sample_search_data, sample_vstats):
        """Bug 3: profile dict was missing 'email' field in scrape()."""
        profile = self._make_profile(sample_channel_details, sample_search_data, sample_vstats)
        assert "email" in profile
        assert profile["email"] == "test@example.com"

    def test_email_empty_when_not_in_details(self, sample_search_data, sample_vstats):
        details = {"followers": 1000}
        profile = self._make_profile(details, sample_search_data, sample_vstats)
        assert profile["email"] == ""

    def test_platform_is_youtube(self, sample_channel_details, sample_search_data, sample_vstats):
        profile = self._make_profile(sample_channel_details, sample_search_data, sample_vstats)
        assert profile["platform"] == "YouTube"
