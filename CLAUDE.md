# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

YouTube Creator Scraper — a Python/Streamlit app for discovering and scoring YouTube channels by keyword relevance, engagement, growth, and publishing frequency. Used for influencer research (originally built for Sorare). Outputs styled Excel reports.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Install with dev tools (pytest, ruff)
pip install -e ".[dev]"

# Run the Streamlit web UI
streamlit run app.py

# Run via CLI
python youtube_scraper.py --keywords "Sorare" --region FR --days 90
python youtube_scraper.py --keywords "Sorare" "NFT" --region FR --days 90 --max-channels 200 --output results.xlsx
python youtube_scraper.py --keywords "Sorare" --no-video-stats  # faster, skips per-channel video fetching

# Run tests
pytest
pytest -v  # verbose

# Lint
ruff check .
ruff format .  # auto-format
```

## Architecture

Two main Python files + a pytest test suite:

- **`app.py`** — Streamlit web UI. Sidebar collects search parameters (keywords, region, language, date range, follower filters). Calls shared functions from `youtube_scraper`, displays results with metrics and charts, offers Excel download. Contains only UI logic — no business logic.
- **`youtube_scraper.py`** — Core logic. Contains:
  - **Constants** — `TIER_BOUNDARIES`, `SCORE_WEIGHTS`, threshold tables, rate limits, etc. All magic numbers are named constants.
  - **API functions** — `search_videos_by_keyword()`, `get_channel_details()`, `get_recent_video_stats()`
  - **Scoring** — `_score_from_thresholds()` generic helper, `score_engagement/pertinence/regularite/croissance()`, `compute_scores()`
  - **Shared helpers** — `merge_keyword_results()`, `compute_channel_metrics()`, `build_channel_profile()` — used by both `scrape()` and `app.py` to eliminate duplication
  - **Excel export** — `export_excel()` with openpyxl styling
  - **CLI** — `main()` with argparse, serves as CLI entry point
- **`tests/`** — pytest test suite covering scoring, tiers, metrics, profile building, Excel export, and CLI argument parsing.

## Key Technical Details

**Scoring formula** (with video stats enabled):
`score_global = pertinence×0.37 + engagement×0.28 + croissance×0.20 + regularite×0.15`

Without video stats, weights renormalize to exclude engagement. Weights are defined in `SCORE_WEIGHTS` constant.

**Tier system**: nano (<1K), micro (1K-10K), mid (10K-100K), macro (100K-1M), mega (1M+). Defined in `TIER_BOUNDARIES` constant.

**Rate limiting**: Sleep-based throttling between API calls. Delays defined as `RATE_LIMIT_SEARCH`, `RATE_LIMIT_CHANNEL_DETAILS`, `RATE_LIMIT_VIDEO_STATS` constants. YouTube API daily quota is 10,000 units; search costs 100 units per page.

**Constants approach**: All thresholds, weights, and magic numbers are named constants at the top of `youtube_scraper.py`. When modifying scoring or tier logic, update the constants rather than hardcoding values.

## Best Practices

- All business logic belongs in `youtube_scraper.py` — `app.py` should only contain UI code
- Use the shared functions (`merge_keyword_results`, `compute_channel_metrics`, `build_channel_profile`) to avoid duplication
- Use named constants from the constants block; never hardcode thresholds or weights
- Add type hints to all new functions
- Run `ruff check . && pytest` before committing

## Environment

- **Python 3.12+**
- Requires `YOUTUBE_API_KEY` — set in `.env` file (see `.env.example`) or pass via `--api-key` CLI flag or enter in the Streamlit sidebar.
