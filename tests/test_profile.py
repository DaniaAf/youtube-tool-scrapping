from youtube_scraper import (
    COLUMNS,
    ZERO_VIDEO_STATS,
    _parse_iso8601_duration,
    _parse_topic_categories,
    build_channel_profile,
    compute_channel_metrics,
)


class TestBuildChannelProfile:
    def _make_profile(self, details, search_data, vstats, has_video_stats=True, days=90):
        metrics = compute_channel_metrics(details, vstats, search_data, days)
        return build_channel_profile(
            "UC_TEST_123",
            details,
            search_data,
            metrics,
            has_video_stats,
            "2025-01-01 12:00:00",
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
            sample_channel_details,
            sample_search_data,
            dict(ZERO_VIDEO_STATS),
            has_video_stats=False,
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

    def test_content_categories_from_details(self, sample_channel_details, sample_search_data, sample_vstats):
        profile = self._make_profile(sample_channel_details, sample_search_data, sample_vstats)
        assert profile["content_categories"] == "Gaming, Sports"

    def test_channel_keywords_from_details(self, sample_channel_details, sample_search_data, sample_vstats):
        profile = self._make_profile(sample_channel_details, sample_search_data, sample_vstats)
        assert profile["channel_keywords"] == "sorare fantasy football"

    def test_content_categories_empty_when_missing(self, sample_search_data, sample_vstats):
        details = {"followers": 1000}
        profile = self._make_profile(details, sample_search_data, sample_vstats)
        assert profile["content_categories"] == ""
        assert profile["channel_keywords"] == ""


class TestParseTopicCategories:
    def test_parses_wikipedia_urls(self):
        topic_details = {
            "topicCategories": [
                "https://en.wikipedia.org/wiki/Gaming",
                "https://en.wikipedia.org/wiki/Sports",
            ]
        }
        assert _parse_topic_categories(topic_details) == ["Gaming", "Sports"]

    def test_handles_underscores_in_labels(self):
        topic_details = {
            "topicCategories": ["https://en.wikipedia.org/wiki/Video_game"]
        }
        assert _parse_topic_categories(topic_details) == ["Video game"]

    def test_empty_topic_details(self):
        assert _parse_topic_categories({}) == []

    def test_no_wiki_urls(self):
        topic_details = {"topicCategories": ["https://example.com/something"]}
        assert _parse_topic_categories(topic_details) == []


class TestParseISO8601Duration:
    def test_seconds_only(self):
        assert _parse_iso8601_duration("PT45S") == 45

    def test_minutes_and_seconds(self):
        assert _parse_iso8601_duration("PT2M30S") == 150

    def test_hours_minutes_seconds(self):
        assert _parse_iso8601_duration("PT1H2M3S") == 3723

    def test_minutes_only(self):
        assert _parse_iso8601_duration("PT10M") == 600

    def test_hours_only(self):
        assert _parse_iso8601_duration("PT1H") == 3600

    def test_empty_string(self):
        assert _parse_iso8601_duration("") == 0

    def test_invalid_format(self):
        assert _parse_iso8601_duration("not-a-duration") == 0

    def test_shorts_boundary_60s(self):
        assert _parse_iso8601_duration("PT1M") == 60  # exactly 60s = short

    def test_just_over_shorts(self):
        assert _parse_iso8601_duration("PT1M1S") == 61  # 61s = long form


class TestShortsInProfile:
    def _make_profile(self, details, search_data, vstats, has_video_stats=True, days=90):
        metrics = compute_channel_metrics(details, vstats, search_data, days)
        return build_channel_profile(
            "UC_TEST_123",
            details,
            search_data,
            metrics,
            has_video_stats,
            "2025-01-01 12:00:00",
        )

    def test_shorts_fields_present(self, sample_channel_details, sample_search_data):
        vstats = {"views": 1000, "likes": 50, "comments": 5, "video_count": 10, "shorts_count": 3, "long_form_count": 7}
        profile = self._make_profile(sample_channel_details, sample_search_data, vstats)
        assert profile["shorts_count"] == 3
        assert profile["long_form_count"] == 7
        assert profile["shorts_ratio"] == 0.3

    def test_shorts_ratio_zero_when_no_videos(self, sample_channel_details, sample_search_data):
        vstats = dict(ZERO_VIDEO_STATS)
        profile = self._make_profile(sample_channel_details, sample_search_data, vstats)
        assert profile["shorts_ratio"] == 0.0
