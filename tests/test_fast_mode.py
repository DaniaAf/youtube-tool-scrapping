from unittest.mock import MagicMock

from googleapiclient.errors import HttpError

from youtube_scraper import ZERO_VIDEO_STATS, get_video_stats_batch, merge_keyword_results


class TestGetVideoStatsBatch:
    def test_empty_video_ids(self):
        youtube = MagicMock()
        result = get_video_stats_batch(youtube, [])
        assert result == ZERO_VIDEO_STATS
        youtube.videos.assert_not_called()

    def test_single_batch(self):
        youtube = MagicMock()
        item_5m = {
            "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "10"},
            "contentDetails": {"duration": "PT5M"},
        }
        item_30s = {
            "statistics": {"viewCount": "2000", "likeCount": "100", "commentCount": "20"},
            "contentDetails": {"duration": "PT30S"},
        }
        youtube.videos.return_value.list.return_value.execute.return_value = {"items": [item_5m, item_30s]}
        result = get_video_stats_batch(youtube, ["v1", "v2"])
        assert result["views"] == 3000
        assert result["likes"] == 150
        assert result["comments"] == 30
        assert result["video_count"] == 2
        assert result["shorts_count"] == 1  # PT30S
        assert result["long_form_count"] == 1  # PT5M

    def test_multi_batch(self):
        """55 videos should trigger 2 batches (50 + 5)."""
        youtube = MagicMock()
        item_100 = {
            "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
            "contentDetails": {"duration": "PT10M"},
        }
        item_200 = {
            "statistics": {"viewCount": "200", "likeCount": "10", "commentCount": "2"},
            "contentDetails": {"duration": "PT45S"},
        }
        batch1 = {"items": [item_100 for _ in range(50)]}
        batch2 = {"items": [item_200 for _ in range(5)]}
        youtube.videos.return_value.list.return_value.execute = MagicMock(side_effect=[batch1, batch2])

        video_ids = [f"v{i}" for i in range(55)]
        result = get_video_stats_batch(youtube, video_ids)
        assert result["views"] == 50 * 100 + 5 * 200  # 6000
        assert result["likes"] == 50 * 5 + 5 * 10  # 300
        assert result["comments"] == 50 * 1 + 5 * 2  # 60
        assert result["video_count"] == 55
        assert result["shorts_count"] == 5  # batch2 items are shorts
        assert result["long_form_count"] == 50  # batch1 items are long form

    def test_http_error_skips_batch(self):
        """HttpError on one batch should skip it but continue with others."""
        youtube = MagicMock()
        item_ok = {
            "statistics": {"viewCount": "500", "likeCount": "25", "commentCount": "5"},
            "contentDetails": {"duration": "PT3M"},
        }
        batch_ok = {"items": [item_ok for _ in range(3)]}
        # Use 400 (not retryable) so tenacity doesn't retry
        http_error = HttpError(resp=MagicMock(status=400), content=b"Bad Request")
        youtube.videos.return_value.list.return_value.execute = MagicMock(side_effect=[http_error, batch_ok])

        video_ids = [f"v{i}" for i in range(55)]  # triggers 2 batches
        result = get_video_stats_batch(youtube, video_ids)
        # First batch fails, second batch returns 3 items
        assert result["views"] == 1500
        assert result["video_count"] == 3

    def test_missing_stats_fields_default_to_zero(self):
        youtube = MagicMock()
        youtube.videos.return_value.list.return_value.execute.return_value = {
            "items": [
                {"statistics": {}, "contentDetails": {"duration": "PT10M"}},
                {"statistics": {"viewCount": "100"}, "contentDetails": {"duration": "PT20S"}},
            ]
        }
        result = get_video_stats_batch(youtube, ["v1", "v2"])
        assert result["views"] == 100
        assert result["likes"] == 0
        assert result["comments"] == 0
        assert result["video_count"] == 2
        assert result["shorts_count"] == 1
        assert result["long_form_count"] == 1


class TestMergeKeywordResultsDedup:
    def test_deduplicates_video_ids(self):
        all_channels = {
            "ch1": {
                "channel_id": "ch1",
                "display_name": "Channel 1",
                "video_ids": ["v1", "v2", "v3"],
                "mentions_count": 2,
            }
        }
        new_channels = {
            "ch1": {
                "channel_id": "ch1",
                "display_name": "Channel 1",
                "video_ids": ["v2", "v3", "v4"],  # v2, v3 overlap
                "mentions_count": 1,
            }
        }
        merge_keyword_results(all_channels, new_channels)
        assert all_channels["ch1"]["mentions_count"] == 3
        assert sorted(all_channels["ch1"]["video_ids"]) == ["v1", "v2", "v3", "v4"]

    def test_no_overlap(self):
        all_channels = {
            "ch1": {
                "channel_id": "ch1",
                "display_name": "Channel 1",
                "video_ids": ["v1"],
                "mentions_count": 1,
            }
        }
        new_channels = {
            "ch1": {
                "channel_id": "ch1",
                "display_name": "Channel 1",
                "video_ids": ["v2"],
                "mentions_count": 1,
            }
        }
        merge_keyword_results(all_channels, new_channels)
        assert all_channels["ch1"]["video_ids"] == ["v1", "v2"]

    def test_new_channel_added(self):
        all_channels = {}
        new_channels = {
            "ch1": {
                "channel_id": "ch1",
                "display_name": "Channel 1",
                "video_ids": ["v1"],
                "mentions_count": 1,
            }
        }
        merge_keyword_results(all_channels, new_channels)
        assert "ch1" in all_channels
        assert all_channels["ch1"]["video_ids"] == ["v1"]
