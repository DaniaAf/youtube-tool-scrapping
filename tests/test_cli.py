from unittest.mock import patch

from youtube_scraper import main


class TestCLI:
    @patch("youtube_scraper.scrape")
    def test_minimal_args(self, mock_scrape):
        with patch("sys.argv", ["youtube_scraper.py", "--keywords", "Sorare"]):
            main()
        mock_scrape.assert_called_once_with(
            keywords=["Sorare"],
            region_code="FR",
            days=90,
            language=None,
            api_key=None,
            output_file="youtube_profiles.xlsx",
            max_channels=150,
            fetch_video_stats=True,
        )

    @patch("youtube_scraper.scrape")
    def test_all_args(self, mock_scrape):
        with patch("sys.argv", [
            "youtube_scraper.py",
            "--keywords", "Sorare", "NFT",
            "--region", "US",
            "--days", "30",
            "--language", "en",
            "--output", "out.xlsx",
            "--max-channels", "50",
            "--api-key", "test-key",
        ]):
            main()
        mock_scrape.assert_called_once_with(
            keywords=["Sorare", "NFT"],
            region_code="US",
            days=30,
            language="en",
            api_key="test-key",
            output_file="out.xlsx",
            max_channels=50,
            fetch_video_stats=True,
        )

    @patch("youtube_scraper.scrape")
    def test_no_video_stats_flag(self, mock_scrape):
        with patch("sys.argv", ["youtube_scraper.py", "--keywords", "test", "--no-video-stats"]):
            main()
        mock_scrape.assert_called_once()
        assert mock_scrape.call_args.kwargs["fetch_video_stats"] is False
