"""
YouTube Creator Scraper — Streamlit Web UI
Launch: streamlit run app.py
"""

import io
import json
import math
import os
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config file persistence (replaces browser localStorage)
# ---------------------------------------------------------------------------

_CONFIG_DIR = Path.home() / ".youtube_scraper"
_CONFIG_FILE = _CONFIG_DIR / "config.json"


def _load_api_key() -> str:
    """Load API key from config file."""
    if _CONFIG_FILE.exists():
        try:
            data = json.loads(_CONFIG_FILE.read_text())
            return data.get("api_key", "")
        except (json.JSONDecodeError, OSError):
            return ""
    return ""


def _save_api_key(key: str) -> None:
    """Save API key to config file."""
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _CONFIG_FILE.write_text(json.dumps({"api_key": key}))


def _delete_api_key() -> None:
    """Delete API key from config file."""
    if _CONFIG_FILE.exists():
        _CONFIG_FILE.write_text(json.dumps({"api_key": ""}))


from googleapiclient.errors import HttpError  # noqa: E402

from youtube_scraper import (  # noqa: E402
    COLUMNS,
    QUOTA_COST_LIST,
    QUOTA_COST_SEARCH,
    RATE_LIMIT_VIDEO_STATS,
    SCORE_WEIGHTS,
    YOUTUBE_DAILY_QUOTA,
    ZERO_VIDEO_STATS,
    build_channel_profile,
    clear_cache,
    compute_channel_metrics,
    export_csv,
    export_excel,
    export_json,
    get_channel_details,
    get_quota_used,
    get_recent_video_stats,
    get_video_stats_batch,
    compute_local_confidence,
    get_youtube_client,
    merge_keyword_results,
    reset_quota,
    resolve_channel_urls,
    search_videos_by_keyword,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Sorare YouTube Scraper",
    page_icon="https://pbs.twimg.com/profile_images/1770433750944047104/F7rQNnEi_400x400.jpg",
    layout="wide",
    initial_sidebar_state="collapsed",
)

LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "Sorare26-Logo-Black.png")

# ---------------------------------------------------------------------------
# Design System
# ---------------------------------------------------------------------------

TIER_COLORS = {
    "mega": {"color": "#7C3AED", "bg": "#EDE9FE"},
    "macro": {"color": "#3B82F6", "bg": "#DBEAFE"},
    "mid": {"color": "#10B981", "bg": "#D1FAE5"},
    "micro": {"color": "#F59E0B", "bg": "#FEF3C7"},
    "nano": {"color": "#6B7280", "bg": "#F3F4F6"},
}

TIER_ORDER = ["mega", "macro", "mid", "micro", "nano"]

REGION_OPTIONS = {
    "Worldwide": None,
    "France (FR)": "FR",
    "Belgium (BE)": "BE",
    "Switzerland (CH)": "CH",
    "United Kingdom (GB)": "GB",
    "United States (US)": "US",
    "Germany (DE)": "DE",
    "Spain (ES)": "ES",
    "Italy (IT)": "IT",
    "Brazil (BR)": "BR",
    "Canada (CA)": "CA",
}

# Languages expected for each region — used to filter out off-target creators
REGION_LANGUAGES = {
    "FR": {"fr"},
    "BE": {"fr", "nl", "de"},
    "CH": {"fr", "de", "it"},
    "GB": {"en"},
    "US": {"en"},
    "DE": {"de"},
    "ES": {"es"},
    "IT": {"it"},
    "BR": {"pt"},
    "CA": {"en", "fr"},
}

LANGUAGE_OPTIONS = ["All languages", "fr", "en", "de", "es", "it", "pt"]

# Primary language auto-applied per region for relevanceLanguage API param
REGION_PRIMARY_LANGUAGE = {
    "FR": "fr", "BE": "fr", "CH": "fr",
    "GB": "en", "US": "en", "CA": "en",
    "DE": "de", "ES": "es", "IT": "it", "BR": "pt",
}

FOLLOWER_MIN_OPTIONS = {
    "No minimum": 0,
    "1K+ (Micro)": 1_000,
    "10K+ (Mid)": 10_000,
    "100K+ (Macro)": 100_000,
    "1M+ (Mega)": 1_000_000,
}

FOLLOWER_MAX_OPTIONS = {
    "No maximum": 0,
    "1K (Nano)": 1_000,
    "10K (Micro)": 10_000,
    "100K (Mid)": 100_000,
    "1M (Macro)": 1_000_000,
}

# Label mapping: data key -> English UI label
LABEL_MAP = {
    "keyword_mentions": "Keyword Mentions",
    "score_pertinence": "Relevance",
    "score_engagement": "Engagement",
    "score_croissance": "Growth",
    "score_regularite": "Regularity",
    "score_audience_quality": "Audience Quality",
    "score_shorts_content": "Content Format",
    "views_trend_pct": "Views Trend",
    "engagement_rate_pct": "Engagement %",
    "posts_per_week": "Posts/week",
    "score_global": "Global Score",
    "followers": "Followers",
    "tier": "Tier",
    "display_name": "Channel",
    "is_emerging": "Emerging",
    "status": "Status",
    "punch_above_weight": "Punch Above Weight",
    "punch_above_weight_ratio": "PAW Ratio",
}


_FOLLOWER_SUFFIX_RE = re.compile(r"^\s*([\d.]+)\s*([kKmM])?\s*$")


def _parse_follower_input(label: str, presets: dict[str, int]) -> int:
    """Parse a follower selectbox value: preset label, raw number, or K/M suffix."""
    if label in presets:
        return presets[label]
    m = _FOLLOWER_SUFFIX_RE.match(label)
    if m:
        value = float(m.group(1))
        suffix = (m.group(2) or "").upper()
        if suffix == "K":
            value *= 1_000
        elif suffix == "M":
            value *= 1_000_000
        return int(value)
    st.warning(f"Invalid follower value: '{label}' — defaulting to 0")
    return 0


def inject_css():
    st.markdown(
        """
<style>
    /* --- Page background --- */
    .stApp { background-color: #F8FAFC; }

    /* --- Hide default sidebar hamburger when collapsed --- */
    [data-testid="collapsedControl"] { display: none; }

    /* --- Header --- */
    .app-header {
        display: flex; align-items: center; gap: 16px;
        padding: 12px 0 8px 0; margin-bottom: 4px;
    }
    .app-header img { height: 36px; }
    .app-header .tagline {
        color: #64748B; font-size: 14px; margin-left: auto;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }

    /* --- Card --- */
    .card {
        background: #FFFFFF; border: 1px solid #E2E8F0;
        border-radius: 12px; padding: 24px; margin-bottom: 16px;
    }

    /* --- KPI metric cards --- */
    .kpi-card {
        background: #FFFFFF; border: 1px solid #E2E8F0;
        border-radius: 10px; padding: 16px 20px; text-align: center;
    }
    .kpi-card .kpi-value {
        font-size: 28px; font-weight: 700; color: #0F172A;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .kpi-card .kpi-label {
        font-size: 13px; color: #64748B; margin-top: 4px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }

    /* --- Tier badge --- */
    .tier-badge {
        display: inline-block; padding: 3px 10px; border-radius: 12px;
        font-size: 12px; font-weight: 600; text-transform: uppercase;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }

    /* --- Score bar --- */
    .score-bar-bg {
        background: #E2E8F0; border-radius: 6px; height: 10px; width: 100%;
    }
    .score-bar-fill {
        border-radius: 6px; height: 10px;
    }

    /* --- Channel detail metric --- */
    .detail-metric {
        background: #F8FAFC; border: 1px solid #E2E8F0;
        border-radius: 8px; padding: 12px; text-align: center;
    }
    .detail-metric .dm-value {
        font-size: 22px; font-weight: 700; color: #0F172A;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .detail-metric .dm-label {
        font-size: 12px; color: #64748B;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }

    /* --- Methodology weight bar --- */
    .weight-bar {
        display: flex; align-items: center; gap: 8px; margin-bottom: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .weight-bar .wb-label { width: 100px; font-size: 14px; color: #0F172A; font-weight: 500; }
    .weight-bar .wb-bar { height: 20px; border-radius: 4px; }
    .weight-bar .wb-pct { font-size: 13px; color: #64748B; width: 40px; }

    /* --- Progress button override --- */
    .stButton > button[kind="primary"] {
        background: #000000 !important; color: #ffffff !important;
        font-weight: 600 !important; border: none !important;
        border-radius: 8px !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: #333333 !important;
    }

    /* --- Download button override --- */
    .stDownloadButton > button[kind="primary"] {
        background: #000000 !important; color: #ffffff !important;
        font-weight: 600 !important; border: none !important;
        border-radius: 8px !important;
    }
    .stDownloadButton > button[kind="primary"]:hover {
        background: #333333 !important;
    }

    /* --- Spacer between KPI strip and toolbar --- */
    .kpi-strip-spacer { margin-bottom: 20px; }

    /* --- Section header --- */
    .section-header {
        font-size: 18px; font-weight: 600; color: #0F172A;
        margin: 24px 0 12px 0;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
</style>
    """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def score_color(score: float) -> str:
    if score >= 80:
        return "#000000"
    if score >= 60:
        return "#10B981"
    if score >= 40:
        return "#F59E0B"
    if score >= 20:
        return "#F97316"
    return "#EF4444"


def tier_badge_html(tier: str) -> str:
    tc = TIER_COLORS.get(tier, {"color": "#6B7280", "bg": "#F3F4F6"})
    return f'<span class="tier-badge" style="color:{tc["color"]};background:{tc["bg"]}">{tier}</span>'


def format_followers(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def kpi_card(value: str, label: str) -> str:
    return f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>
    """


def score_bar_html(score: float, label: str, max_val: float = 100) -> str:
    pct = min(score / max_val * 100, 100) if max_val > 0 else 0
    color = score_color(score)
    return f"""
    <div style="margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;margin-bottom:3px">
            <span style="font-size:13px;color:#0F172A;font-weight:500">{label}</span>
            <span style="font-size:13px;color:#64748B;font-weight:600">{score:.0f}</span>
        </div>
        <div class="score-bar-bg">
            <div class="score-bar-fill" style="width:{pct:.0f}%;background:{color}"></div>
        </div>
    </div>
    """


# ---------------------------------------------------------------------------
# Header & Settings
# ---------------------------------------------------------------------------


@st.dialog("Settings")
def show_settings():
    st.markdown("#### YouTube API Key")

    current_key = st.session_state.get("api_key", "")
    if current_key:
        masked = current_key[:4] + "..." + current_key[-4:] if len(current_key) > 8 else "****"
        st.success(f"Key saved: `{masked}`")
    else:
        st.warning("No API key configured.")

    new_key = st.text_input("Enter API key", type="password", placeholder="AIza...")

    btn_left, btn_right = st.columns(2)
    with btn_left:
        if st.button("Save", use_container_width=True, type="primary", disabled=not new_key):
            _save_api_key(new_key)
            st.session_state["api_key"] = new_key
            st.rerun()
    with btn_right:
        if st.button("Delete", use_container_width=True, disabled=not current_key):
            _delete_api_key()
            st.session_state.pop("api_key", None)
            st.rerun()

    st.markdown("---")
    st.markdown("#### How to get a YouTube API key")
    st.markdown(
        "1. Go to [Google Cloud Console](https://console.cloud.google.com/)\n"
        "2. Create a new project (or select an existing one)\n"
        "3. Enable **YouTube Data API v3** in *APIs & Services > Library*\n"
        "4. Go to *APIs & Services > Credentials* and click **Create Credentials > API Key**\n"
        "5. Copy the key and paste it above"
    )
    st.caption("Daily quota: 10,000 units. For security, restrict the key to YouTube Data API v3 only.")

    st.markdown("---")
    st.markdown("#### Cache")
    st.caption(
        "API responses are cached locally to save quota. Search results: 4h, channel details: 24h, video stats: 4h."
    )
    if st.button("Clear Cache", use_container_width=True):
        clear_cache()
        st.success("Cache cleared.")

    st.markdown("---")
    st.markdown("#### Quota Counter")
    used = get_quota_used()
    st.caption(f"Tracked usage today: ~{_format_quota(used)} / {_format_quota(YOUTUBE_DAILY_QUOTA)} units")
    if st.button("Reset Quota Counter", use_container_width=True):
        reset_quota()
        st.success("Quota counter reset.")


def _format_quota(n: int) -> str:
    """Format a quota number: 0, 450, 1.2K, 10K etc."""
    if n >= 1_000:
        return f"{n / 1_000:.1f}K".replace(".0K", "K")
    return str(n)


def render_header():
    has_key = bool(st.session_state.get("api_key"))
    status_color = "#10B981" if has_key else "#EF4444"
    # Style the status tertiary button as colored text
    st.markdown(
        f'<style>button[kind="tertiary"] {{ color: {status_color} !important; font-weight: 600 !important; font-size: 13px !important; }}</style>',
        unsafe_allow_html=True,
    )
    col_logo, col_right = st.columns([7, 3])
    with col_logo:
        st.image(LOGO_PATH, width=160)
        st.caption("YouTube Creator Scraper — Discover and score creators by keyword relevance, engagement, and growth")
    with col_right:
        _, col_status, col_settings = st.columns([0.5, 1, 1], gap="small")
        with col_status:
            if has_key:
                used = get_quota_used()
                label = f"API Connected · ~{_format_quota(used)} / {_format_quota(YOUTUBE_DAILY_QUOTA)}"
            else:
                label = "No API Key"
            if st.button(label, key="api_status_btn", type="tertiary", use_container_width=True):
                show_settings()
        with col_settings:
            if st.button("Settings", icon=":material/settings:", use_container_width=True):
                show_settings()


# ---------------------------------------------------------------------------
# Search config card
# ---------------------------------------------------------------------------


def render_search_config():
    with st.container(border=True):
        # Row 1: Keywords | Region | Period
        col_kw, col_region, col_period = st.columns([5, 2, 2])

        with col_kw:
            keywords_raw = st.text_input(
                "Keywords (comma-separated)",
                value=st.session_state.get("keywords_raw", "Sorare"),
                help="Separate multiple keywords with commas. Each triggers a separate search.",
                key="kw_input",
            )

        with col_region:
            region_label = st.selectbox(
                "Region",
                options=list(REGION_OPTIONS.keys()),
                index=0,
                help=(
                    "Target a specific market. The language is automatically applied based on the region "
                    "(e.g. France → French content, US → English content). "
                    "Creators with no matching signals are filtered out."
                ),
            )

        with col_period:
            days = st.slider(
                "Period (months)",
                min_value=1,
                max_value=24,
                value=3,
                step=1,
                help="Time window for video publication analysis. Wider = more results but slower and more quota.",
            )
            days = days * 30

        # Row 2: Min foll | Max foll | Max ch/kw | Video stats+quota | Search+DL
        r2_min, r2_max, r2_ch, r2_stats, r2_btn = st.columns([2, 2, 2, 2, 2])

        with r2_min:
            min_label = st.selectbox(
                "Min Followers",
                options=list(FOLLOWER_MIN_OPTIONS.keys()),
                index=0,
                accept_new_options=True,
                help="Select a preset or type a custom value (e.g. 5000, 5K, 1.5M)",
            )
            followers_min = _parse_follower_input(min_label, FOLLOWER_MIN_OPTIONS)

        with r2_max:
            max_label = st.selectbox(
                "Max Followers",
                options=list(FOLLOWER_MAX_OPTIONS.keys()),
                index=0,
                accept_new_options=True,
                help="Select a preset or type a custom value (e.g. 5000, 5K, 1.5M)",
            )
            followers_max = _parse_follower_input(max_label, FOLLOWER_MAX_OPTIONS)

        with r2_ch:
            max_channels = st.slider(
                "Results per keyword",
                min_value=10,
                max_value=300,
                value=100,
                step=10,
                help="Maximum number of unique creators to analyse per keyword. Higher = more results but more quota consumed. 100 is a good starting point.",
            )

        with r2_stats:
            stats_mode = st.selectbox(
                "Video stats mode",
                options=["Fast", "Full", "None"],
                index=0,
                help="Fast: reuse search video IDs (~1 unit/batch). Full: per-channel search (~100 units/ch). None: skip.",
            )
            kw_count = len([k for k in keywords_raw.split(",") if k.strip()])
            pages_per_kw = math.ceil(max_channels / 30)
            search_cost = kw_count * pages_per_kw * QUOTA_COST_SEARCH
            channel_cost = math.ceil(max_channels / 50) * QUOTA_COST_LIST
            if stats_mode == "Full":
                video_cost = max_channels * (QUOTA_COST_SEARCH + QUOTA_COST_LIST)
            elif stats_mode == "Fast":
                video_cost = math.ceil(max_channels * 5 / 50) * QUOTA_COST_LIST
            else:
                video_cost = 0
            quota_est = search_cost + channel_cost + video_cost
            st.caption(f"Est. quota: ~{quota_est:,} / 10K")

        with r2_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            run_btn = st.button("Search", use_container_width=True, type="primary")

    return {
        "run_btn": run_btn,
        "keywords_raw": keywords_raw,
        "region": REGION_OPTIONS[region_label],
        "days": days,
        "api_key": st.session_state.get("api_key", ""),
        "language": REGION_PRIMARY_LANGUAGE.get(REGION_OPTIONS[region_label], None),
        "followers_min": followers_min,
        "followers_max": followers_max,
        "max_channels": max_channels,
        "stats_mode": stats_mode.lower(),  # "fast", "full", or "none"
        "output_name": f"youtube_{datetime.now().strftime('%Y%m%d')}.xlsx",
    }


# ---------------------------------------------------------------------------
# Empty state / onboarding
# ---------------------------------------------------------------------------


def render_empty_state():
    st.markdown(
        """
        <div style="max-width:860px;margin:48px auto 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
            <h2 style="text-align:center;font-size:22px;font-weight:700;color:#1E3A5F;margin-bottom:4px;">
                How to find YouTube creators
            </h2>
            <p style="text-align:center;color:#64748B;font-size:14px;margin-bottom:32px;">
                Follow these 3 steps to get your first results in minutes.
            </p>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:32px;">
                <div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:12px;padding:20px;">
                    <div style="font-size:28px;margin-bottom:8px;">🔑</div>
                    <div style="font-weight:700;color:#1E3A5F;font-size:14px;margin-bottom:6px;">Step 1 — API Key</div>
                    <div style="color:#475569;font-size:13px;line-height:1.6;">
                        Click <strong>Settings</strong> in the header and paste your
                        <strong>YouTube Data API v3</strong> key. Get one free at
                        Google Cloud Console (10,000 quota/day).
                    </div>
                </div>
                <div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:12px;padding:20px;">
                    <div style="font-size:28px;margin-bottom:8px;">🔍</div>
                    <div style="font-weight:700;color:#1E3A5F;font-size:14px;margin-bottom:6px;">Step 2 — Configure your search</div>
                    <div style="color:#475569;font-size:13px;line-height:1.6;">
                        Enter <strong>keywords</strong> (e.g. <em>pack opening, FIFA</em>),
                        pick a <strong>Region</strong> to target an audience market,
                        and set a <strong>Period</strong> to focus on recent activity.
                    </div>
                </div>
                <div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:12px;padding:20px;">
                    <div style="font-size:28px;margin-bottom:8px;">📊</div>
                    <div style="font-weight:700;color:#1E3A5F;font-size:14px;margin-bottom:6px;">Step 3 — Analyse & Export</div>
                    <div style="color:#475569;font-size:13px;line-height:1.6;">
                        Creators are automatically <strong>scored</strong> on relevance,
                        engagement, growth and regularity. Click any row for details,
                        then export to <strong>Excel, CSV or JSON</strong>.
                    </div>
                </div>
            </div>
            <div style="background:#EFF6FF;border:1px solid #BFDBFE;border-radius:12px;padding:20px 24px;margin-bottom:32px;">
                <div style="font-weight:700;color:#1E3A5F;font-size:14px;margin-bottom:10px;">Tips for better results</div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
                    <div style="color:#475569;font-size:13px;">
                        <strong>Multiple keywords</strong> — separate with commas to combine searches
                        (e.g. <em>Sorare, fantasy football, NFT sport</em>)
                    </div>
                    <div style="color:#475569;font-size:13px;">
                        <strong>Region filter</strong> — targets creators whose audience is in that market,
                        not just the creator's location
                    </div>
                    <div style="color:#475569;font-size:13px;">
                        <strong>Video stats mode</strong> — <em>Full</em> fetches all videos per creator
                        (costs 100 quota/creator — can drain your daily quota quickly).
                        <em>Fast</em> only analyses recent videos already found during the search — recommended.
                        <em>None</em> disables video stats entirely to save quota.
                    </div>
                    <div style="color:#475569;font-size:13px;">
                        <strong>Keyword mentions</strong> — counts how many of the creator's recent videos
                        and channel bio reference your keyword
                    </div>
                </div>
            </div>
            <p style="text-align:center;color:#94A3B8;font-size:13px;">
                Enter keywords above and click <strong>Search</strong> to get started.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Progress UX
# ---------------------------------------------------------------------------


def run_search(config):
    keywords = [k.strip() for k in config["keywords_raw"].split(",") if k.strip()]

    if not config["api_key"]:
        st.error(
            "API key is missing. Click Settings in the header to add your YouTube API key, or set YOUTUBE_API_KEY in your .env file."
        )
        return
    if not keywords:
        st.error("Add at least one keyword.")
        return

    with st.status("Searching YouTube...", expanded=True) as status_container:
        try:
            youtube = get_youtube_client(config["api_key"])

            # Step 1: keyword search
            all_channels = {}
            for i, kw in enumerate(keywords):
                st.write(f"Searching keyword: **{kw}** ({i + 1}/{len(keywords)})")
                found = search_videos_by_keyword(
                    youtube,
                    kw,
                    config["region"],
                    config["days"],
                    config["language"],
                    config["max_channels"],
                )
                st.write(f'Found {len(found)} channels for "{kw}"')
                merge_keyword_results(all_channels, found)

            if not all_channels:
                status_container.update(label="No results", state="error")
                st.warning("No channels found. Try different keywords, a longer period, or check your API key.")
                return

            st.write(f"Total unique channels: {len(all_channels)}")

            # Step 2: fetch details
            channel_ids = list(all_channels.keys())[: config["max_channels"]]
            st.write(f"Fetching details for {len(channel_ids)} channels...")
            channel_details = get_channel_details(youtube, channel_ids)

            # Step 3: compute scores
            st.write("Computing scores...")
            collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            profiles = []

            progress = st.progress(0)
            for idx, cid in enumerate(channel_ids):
                details = channel_details.get(cid, {})
                search_data = all_channels[cid]
                followers = details.get("followers", 0)

                # Smart region filter: strict for non-English regions, relaxed for English-speaking ones
                region = config["region"]
                if region:
                    confidence = compute_local_confidence(details, region)
                    english_regions = {"US", "GB", "CA"}
                    min_confidence = "medium" if region in english_regions else "high"
                    if confidence == "low" or (min_confidence == "high" and confidence == "medium"):
                        progress.progress((idx + 1) / len(channel_ids))
                        continue

                # Follower filter
                if config["followers_min"] > 0 and followers < config["followers_min"]:
                    progress.progress((idx + 1) / len(channel_ids))
                    continue
                if config["followers_max"] > 0 and followers > config["followers_max"]:
                    progress.progress((idx + 1) / len(channel_ids))
                    continue

                mode = config["stats_mode"]
                if mode == "full":
                    try:
                        vstats = get_recent_video_stats(youtube, cid, config["days"])
                        time.sleep(RATE_LIMIT_VIDEO_STATS)
                    except HttpError:
                        vstats = dict(ZERO_VIDEO_STATS)
                elif mode == "fast":
                    vstats = get_video_stats_batch(youtube, search_data.get("video_ids", []))
                else:  # "none"
                    vstats = dict(ZERO_VIDEO_STATS)

                # Boost mentions_count if keyword appears in channel bio or channel_keywords
                search_data = dict(all_channels[cid])  # local copy — don't mutate cache
                bio = details.get("bio_snippet", "").lower()
                ch_kw = details.get("channel_keywords", "").lower()
                for kw in keywords:
                    kw_lower = kw.lower()
                    if kw_lower in bio or kw_lower in ch_kw:
                        search_data["mentions_count"] += 1

                has_stats = mode != "none"
                metrics = compute_channel_metrics(details, vstats, search_data, config["days"])
                profile = build_channel_profile(cid, details, search_data, metrics, has_stats, collected_at, config["region"])
                profiles.append(profile)
                progress.progress((idx + 1) / len(channel_ids))

            progress.empty()
            status_container.update(label=f"Done — {len(profiles)} channels scored", state="complete")

        except HttpError as e:
            err_str = str(e)
            status_container.update(label="API Error", state="error")
            if "quotaExceeded" in err_str or "rateLimitExceeded" in err_str:
                st.error(
                    "YouTube daily quota exceeded (10,000 units/day). Wait until midnight Pacific time, disable detailed video stats, or reduce max channels."
                )
            elif "keyInvalid" in err_str or "API key not valid" in err_str:
                st.error("Invalid API key. Check that it's correctly entered.")
            elif "forbidden" in err_str.lower():
                st.error("Access denied. Ensure YouTube Data API v3 is enabled in Google Cloud Console.")
            else:
                st.error(f"YouTube API error: {e}")
            return
        except Exception as e:
            status_container.update(label="Error", state="error")
            st.error(f"Unexpected error: {e}")
            return

    if not profiles:
        st.warning("No channels matched your filters.")
        return

    # Store in session state
    st.session_state["profiles"] = profiles
    st.session_state["df"] = (
        pd.DataFrame(profiles, columns=COLUMNS).sort_values("score_global", ascending=False).reset_index(drop=True)
    )
    st.session_state["has_video_stats"] = config["stats_mode"] != "none"
    st.session_state["search_keywords"] = keywords
    st.session_state["output_name"] = config["output_name"]
    st.rerun()


# ---------------------------------------------------------------------------
# Channel detail dialog
# ---------------------------------------------------------------------------


@st.dialog("Channel Details", width="large")
def show_channel_detail(row):
    # Header
    tier = row.get("tier", "nano")
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:4px">'
        f'<span style="font-size:22px;font-weight:700;color:#0F172A;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">{row.get("display_name", "")}</span>'
        f'{tier_badge_html(tier)}'
        f'<span style="color:#64748B;font-size:14px">@{row.get("username", "")}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    profile_url = row.get("profile_url", "")
    if profile_url:
        st.markdown(f"[Open on YouTube]({profile_url})")

    bio = row.get("bio_snippet", "")
    if bio:
        st.caption(bio[:300])

    email = row.get("email", "")
    if email:
        st.markdown(f"**Email:** {email}")

    aq = row.get("audience_quality", "")
    categories = row.get("content_categories", "")
    ch_kw = row.get("channel_keywords", "")
    meta_parts = []
    if aq and aq != "unknown":
        meta_parts.append(f"**Audience Quality:** {aq.capitalize()}")
    if categories:
        meta_parts.append(f"**Topics:** {categories}")
    if ch_kw:
        meta_parts.append(f"**Channel Keywords:** {ch_kw}")
    if meta_parts:
        st.markdown(" | ".join(meta_parts))

    st.markdown("---")

    # Metric cards
    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1:
        st.markdown(
            f'<div class="detail-metric"><div class="dm-value">{format_followers(row.get("followers", 0))}</div><div class="dm-label">Followers</div></div>',
            unsafe_allow_html=True,
        )
    with mc2:
        st.markdown(
            f'<div class="detail-metric"><div class="dm-value">{row.get("engagement_rate_pct", 0):.2f}%</div><div class="dm-label">Engagement</div></div>',
            unsafe_allow_html=True,
        )
    with mc3:
        st.markdown(
            f'<div class="detail-metric"><div class="dm-value">{row.get("posts_per_week", 0):.1f}</div><div class="dm-label">Posts/week</div></div>',
            unsafe_allow_html=True,
        )
    with mc4:
        vt = row.get("views_trend_pct")
        vt_display = f"{vt:+.1f}%" if vt is not None else "N/A"
        st.markdown(
            f'<div class="detail-metric"><div class="dm-value">{vt_display}</div><div class="dm-label">Views Trend</div></div>',
            unsafe_allow_html=True,
        )

    # Engagement breakdown
    views = row.get("total_recent_views", 0)
    likes = row.get("total_recent_likes", 0)
    comments = row.get("total_recent_comments", 0)
    vid_count = row.get("recent_video_count", 0)
    if views > 0 or likes > 0 or comments > 0:
        eb1, eb2, eb3, eb4 = st.columns(4)
        with eb1:
            st.markdown(
                f'<div class="detail-metric"><div class="dm-value">{views:,}</div><div class="dm-label">Recent Views</div></div>',
                unsafe_allow_html=True,
            )
        with eb2:
            st.markdown(
                f'<div class="detail-metric"><div class="dm-value">{likes:,}</div><div class="dm-label">Recent Likes</div></div>',
                unsafe_allow_html=True,
            )
        with eb3:
            st.markdown(
                f'<div class="detail-metric"><div class="dm-value">{comments:,}</div><div class="dm-label">Recent Comments</div></div>',
                unsafe_allow_html=True,
            )
        with eb4:
            st.markdown(
                f'<div class="detail-metric"><div class="dm-value">{vid_count}</div><div class="dm-label">Videos Analyzed</div></div>',
                unsafe_allow_html=True,
            )

    # Shorts breakdown
    shorts = row.get("shorts_count", 0)
    long_form = row.get("long_form_count", 0)
    shorts_ratio = row.get("shorts_ratio", 0)
    if shorts > 0 or long_form > 0:
        sb1, sb2, sb3 = st.columns(3)
        with sb1:
            st.markdown(
                f'<div class="detail-metric"><div class="dm-value">{shorts}</div><div class="dm-label">Shorts</div></div>',
                unsafe_allow_html=True,
            )
        with sb2:
            st.markdown(
                f'<div class="detail-metric"><div class="dm-value">{long_form}</div><div class="dm-label">Long-form</div></div>',
                unsafe_allow_html=True,
            )
        with sb3:
            st.markdown(
                f'<div class="detail-metric"><div class="dm-value">{shorts_ratio:.0%}</div><div class="dm-label">Shorts Ratio</div></div>',
                unsafe_allow_html=True,
            )

    # Punch Above Weight
    paw_label = row.get("punch_above_weight", "")
    paw_ratio = row.get("punch_above_weight_ratio", 0)
    if paw_label and paw_label != "unknown":
        paw_color = {"exceptional": "#7C3AED", "strong": "#3B82F6", "normal": "#10B981", "below": "#F59E0B", "weak": "#EF4444"}.get(paw_label, "#6B7280")
        st.markdown(
            f'<div class="detail-metric" style="margin-top:8px"><div class="dm-value" style="color:{paw_color}">{paw_ratio:.2f}x</div><div class="dm-label">Punch Above Weight ({paw_label})</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # Score breakdown
    st.markdown(
        '<p style="font-size:15px;font-weight:600;color:#0F172A;margin-bottom:8px">Score Breakdown</p>',
        unsafe_allow_html=True,
    )
    st.markdown(score_bar_html(row.get("score_global", 0), "Global Score"), unsafe_allow_html=True)
    st.markdown(score_bar_html(row.get("score_pertinence", 0), "Relevance"), unsafe_allow_html=True)
    st.markdown(score_bar_html(row.get("score_engagement", 0), "Engagement"), unsafe_allow_html=True)
    st.markdown(score_bar_html(row.get("score_croissance", 0), "Growth"), unsafe_allow_html=True)
    st.markdown(score_bar_html(row.get("score_regularite", 0), "Regularity"), unsafe_allow_html=True)
    st.markdown(score_bar_html(row.get("score_audience_quality", 0), "Audience Quality"), unsafe_allow_html=True)
    st.markdown(score_bar_html(row.get("score_shorts_content", 0), "Content Format"), unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Summary strip
# ---------------------------------------------------------------------------


def render_summary_strip(df: pd.DataFrame):
    kc1, kc2, kc3, kc4, kc5, kc6 = st.columns(6)
    with kc1:
        st.markdown(kpi_card(str(len(df)), "Channels Found"), unsafe_allow_html=True)
    with kc2:
        st.markdown(kpi_card(f"{df['score_global'].mean():.1f}", "Avg Score"), unsafe_allow_html=True)
    with kc3:
        avg_eng = df["engagement_rate_pct"].mean()
        st.markdown(kpi_card(f"{avg_eng:.2f}%", "Avg Engagement"), unsafe_allow_html=True)
    with kc4:
        total_views = int(df["total_recent_views"].sum())
        st.markdown(kpi_card(format_followers(total_views), "Total Views"), unsafe_allow_html=True)
    with kc5:
        with_mentions = int((df["keyword_mentions"] > 0).sum())
        st.markdown(kpi_card(str(with_mentions), "With Mentions"), unsafe_allow_html=True)
    with kc6:
        emerging_count = int(df["is_emerging"].sum())
        st.markdown(kpi_card(str(emerging_count), "Emerging"), unsafe_allow_html=True)

    st.markdown('<div class="kpi-strip-spacer"></div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Creator list
# ---------------------------------------------------------------------------


def render_creator_list(df: pd.DataFrame, has_video_stats: bool):
    # Unified filter + export toolbar
    with st.container(border=True):
        fc1, fc2, fc3, fc4, fc6, fc7 = st.columns(
            [2, 1.5, 0.7, 2.5, 0.8, 1.5], vertical_alignment="bottom"
        )
        with fc1:
            tier_filter = st.multiselect(
                "Tier",
                options=TIER_ORDER,
                default=TIER_ORDER,
                help="Filter by channel tier",
            )
        with fc2:
            min_score = st.slider("Min Score", 0, 100, 0)
        with fc3:
            emerging_only = st.toggle("Emerging only", value=False)
        with fc4:
            name_search = st.text_input("Search by name", placeholder="Type to filter...")

        # Apply filters
        filtered = df.copy()
        if tier_filter:
            filtered = filtered[filtered["tier"].isin(tier_filter)]
        if min_score > 0:
            filtered = filtered[filtered["score_global"] >= min_score]
        if emerging_only:
            filtered = filtered[filtered["is_emerging"]]
        if name_search:
            filtered = filtered[
                filtered["display_name"].str.contains(name_search, case=False, na=False)
            ]

        n_results = len(filtered)

        with fc6:
            dl_format = st.selectbox(
                "Format",
                ["Excel", "CSV", "JSON"],
                key="dl_format",
                label_visibility="collapsed",
            )

        with fc7:
            dl_label = f"Download ({n_results})"
            profiles = st.session_state.get("profiles", [])
            keywords = st.session_state.get("search_keywords", [])
            base_name = datetime.now().strftime("%Y%m%d")

            if dl_format == "Excel":
                buf = io.BytesIO()
                export_excel(profiles, buf, keywords)
                buf.seek(0)
                st.download_button(
                    label=dl_label,
                    data=buf,
                    file_name=f"youtube_{base_name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                    icon=":material/download:",
                )
            elif dl_format == "CSV":
                buf = io.StringIO()
                export_csv(profiles, buf, keywords)
                st.download_button(
                    label=dl_label,
                    data=buf.getvalue(),
                    file_name=f"youtube_{base_name}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    type="primary",
                    icon=":material/download:",
                )
            else:  # JSON
                buf = io.BytesIO()
                export_json(profiles, buf, keywords)
                buf.seek(0)
                st.download_button(
                    label=dl_label,
                    data=buf,
                    file_name=f"youtube_{base_name}.json",
                    mime="application/json",
                    use_container_width=True,
                    type="primary",
                    icon=":material/download:",
                )

    if filtered.empty:
        st.info("No channels match your filters.")
        return

    # Display columns — includes engagement breakdown
    # Compute avg views per video
    filtered = filtered.copy()
    filtered["avg_views"] = (
        filtered.apply(
            lambda r: int(r["total_recent_views"] / r["recent_video_count"])
            if r.get("recent_video_count", 0) > 0 else 0,
            axis=1,
        )
    )

    display_cols = [
        "display_name",
        "tier",
        "followers",
        "score_global",
        "engagement_rate_pct",
        "avg_views",
        "views_trend_pct",
        "posts_per_week",
        "creator_country",
        "target_market",
        "market_match",
        "local_confidence",
        "email",
        "profile_url",
    ]

    col_config = {
        "display_name": st.column_config.TextColumn("Channel", width="medium"),
        "tier": st.column_config.TextColumn("Tier", width="small"),
        "followers": st.column_config.NumberColumn("Followers", format="%d"),
        "score_global": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.0f"),
        "creator_country": st.column_config.TextColumn("Creator Country", width="small", help="Country declared in the YouTube channel profile"),
        "target_market": st.column_config.TextColumn("Target Market", width="small", help="Country selected in your search"),
        "market_match": st.column_config.CheckboxColumn(
            "Local Creator",
            width="small",
            help=(
                "✅ True — the creator's declared country matches your target market (e.g. US creator for a US search).\n"
                "❌ False — the creator is based elsewhere but their content reaches your market (e.g. Indian creator popular in the US).\n"
                "⬜ Empty — the creator hasn't declared a country on their channel."
            ),
        ),
        "local_confidence": st.column_config.TextColumn(
            "Local Confidence",
            width="small",
            help=(
                "Estimated likelihood that this creator is based in / targeting your selected region. "
                "Based on declared country, channel language, bio keywords, and flag emojis.\n"
                "🟢 high — strong signals (declared country + language match)\n"
                "🟡 medium — partial signals (language or bio keyword match)\n"
                "🔴 low — no signals detected\n"
                "⬜ unknown — no region selected (Worldwide)"
            ),
        ),
        "engagement_rate_pct": st.column_config.NumberColumn(
            "Eng. %",
            format="%.2f",
            help="(Likes + Comments) ÷ Views on recent videos in the selected period",
        ),
        "avg_views": st.column_config.NumberColumn(
            "Avg Views",
            format="%d",
            help="Average views per video over the selected period",
        ),
        "per_video_views": st.column_config.LineChartColumn(
            "Views trend",
            help="Nombre de vues par vidéo récente (de la plus ancienne à la plus récente)",
            width="small",
        ),
        "views_trend_pct": st.column_config.NumberColumn(
            "Trend %",
            format="%+.1f",
            help="Views trend: % change between avg views of older vs newer recent videos. Positive = growing audience.",
        ),
        "posts_per_week": st.column_config.NumberColumn("Posts/wk", format="%.1f"),
        "email": st.column_config.TextColumn("Email", width="medium"),
        "profile_url": st.column_config.LinkColumn("YouTube", width="small", display_text="Link"),
    }

    # Map local_confidence to emoji labels for display
    display_df = filtered[display_cols].copy()
    if "local_confidence" in display_df.columns:
        display_df["local_confidence"] = display_df["local_confidence"].map(
            {"high": "🟢 high", "medium": "🟡 medium", "low": "🔴 low", "unknown": "—"}
        ).fillna("—")

    # Interactive table with row selection
    event = st.dataframe(
        display_df,
        use_container_width=True,
        height=500,
        column_config=col_config,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    # Channel detail on row select
    if event and event.selection and event.selection.rows:
        selected_idx = event.selection.rows[0]
        selected_row = filtered.iloc[selected_idx]
        show_channel_detail(selected_row.to_dict())


# ---------------------------------------------------------------------------
# Methodology (collapsible)
# ---------------------------------------------------------------------------


def render_methodology(has_video_stats: bool):
    # --- CSS for the whole methodology section ---
    method_css = """
    <style>
    .method-intro { font-size: 14px; color: #334155; line-height: 1.7;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .method-formula { display: flex; align-items: center; justify-content: center;
        gap: 6px; flex-wrap: wrap; padding: 16px 20px; margin: 12px 0 4px;
        background: #F8FAFC; border-radius: 10px; border: 1px solid #E2E8F0; }
    .mf-piece { display: inline-flex; align-items: center; gap: 4px; padding: 6px 12px;
        border-radius: 6px; font-size: 13px; font-weight: 600;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .mf-op { font-size: 15px; font-weight: 400; color: #94A3B8; }
    .mf-eq { font-size: 15px; font-weight: 700; color: #0F172A; }
    .score-card { padding: 16px 20px; }
    .score-card-title { font-size: 15px; font-weight: 700; margin-bottom: 2px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .score-card-explain { font-size: 13px; color: #475569; line-height: 1.6; margin-bottom: 12px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .score-card-how { font-size: 11px; color: #94A3B8; margin-bottom: 10px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }
    .score-row { display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }
    .score-label { font-size: 12px; color: #334155; min-width: 70px; text-align: right;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-variant-numeric: tabular-nums; }
    .score-track { flex: 1; height: 22px; background: #F1F5F9; border-radius: 6px;
        overflow: hidden; position: relative; }
    .score-fill { height: 100%; border-radius: 6px; display: flex; align-items: center;
        justify-content: flex-end; padding-right: 8px; }
    .score-val { font-size: 11px; font-weight: 600; color: white;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .score-val-dark { font-size: 11px; font-weight: 600; color: #334155;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        position: absolute; right: 8px; top: 50%; transform: translateY(-50%); }
    .tier-row { display: flex; align-items: center; gap: 12px; padding: 8px 0; }
    .tier-row-label { font-size: 14px; color: #0F172A;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .tier-note { font-size: 12px; color: #64748B; line-height: 1.6; margin-top: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    </style>
    """
    st.markdown(method_css, unsafe_allow_html=True)

    # --- How it works intro ---
    st.markdown("#### How the score works")
    st.markdown(
        '<p class="method-intro">'
        "Every channel gets a <strong>global score from 0 to 100</strong>. "
        "It\u2019s a mix of six things we measure about each channel. "
        "Each one has a weight \u2014 the bigger the weight, the more it counts in the final number. "
        "Emerging channels get a +8 bonus on top."
        "</p>",
        unsafe_allow_html=True,
    )

    # --- Visual formula ---
    w = SCORE_WEIGHTS
    full_weights = {
        "Relevance": w["pertinence"],
        "Engagement": w["engagement"],
        "Growth": w["croissance"],
        "Regularity": w["regularite"],
        "Audience Q.": w["audience_quality"],
        "Content Fmt.": w["shorts_content"],
    }
    if has_video_stats:
        weights = full_weights
    else:
        # Without video stats: engagement, growth, shorts_content excluded
        active = {k: v for k, v in full_weights.items() if k not in ("Engagement", "Growth", "Content Fmt.")}
        total = sum(active.values())
        weights = {k: round(v / total, 2) for k, v in active.items()}

    weight_colors = {
        "Relevance": ("#0F172A", "#F1F5F9"),
        "Engagement": ("#3B82F6", "#EFF6FF"),
        "Growth": ("#059669", "#ECFDF5"),
        "Regularity": ("#D97706", "#FFFBEB"),
        "Audience Q.": ("#7C3AED", "#EDE9FE"),
        "Content Fmt.": ("#EC4899", "#FCE7F3"),
    }

    pieces = []
    for name, val in weights.items():
        pct = int(val * 100)
        fg, bg = weight_colors.get(name, ("#64748B", "#F1F5F9"))
        pieces.append(f'<span class="mf-piece" style="color:{fg};background:{bg}">{name} {pct}%</span>')

    formula_html = (
        '<div class="method-formula">'
        '<span class="mf-eq">Score</span> <span class="mf-op">=</span> '
        + ' <span class="mf-op">+</span> '.join(pieces)
        + "</div>"
    )
    st.markdown(formula_html, unsafe_allow_html=True)

    if not has_video_stats:
        st.info("Video stats disabled \u2014 Engagement, Growth, and Content Format are excluded; remaining weights are renormalized.")

    st.markdown("")

    # --- Score cards with explanations ---
    st.markdown("#### What each metric means")

    def _render_score_card(title, icon, color, explanation, rows):
        """Render a visual score card with explanation + colored bars."""
        html = '<div class="score-card">'
        html += f'<div class="score-card-title">{icon} {title}</div>'
        html += f'<div class="score-card-explain">{explanation}</div>'
        html += '<div class="score-card-how">Score scale</div>'
        for label, score in rows:
            bar_w = max(score, 4)
            if score >= 30:
                html += (
                    f'<div class="score-row">'
                    f'<span class="score-label">{label}</span>'
                    f'<div class="score-track"><div class="score-fill" style="width:{bar_w}%;background:{color}">'
                    f'<span class="score-val">{score}</span></div></div>'
                    f'</div>'
                )
            else:
                html += (
                    f'<div class="score-row">'
                    f'<span class="score-label">{label}</span>'
                    f'<div class="score-track"><div class="score-fill" style="width:{bar_w}%;background:{color}"></div>'
                    f'<span class="score-val-dark">{score}</span></div>'
                    f'</div>'
                )
        html += "</div>"
        return html

    th_left, th_right = st.columns(2)

    with th_left:
        with st.container(border=True):
            st.markdown(
                _render_score_card(
                    "Relevance", "\U0001F3AF", "#0F172A",
                    "We search YouTube for your keywords and count how many times "
                    "each channel\u2019s <strong>video titles</strong> mention them. "
                    "More mentions = the channel talks about your topic more often.",
                    [("10+", 100), ("5 \u2013 9", 85), ("3 \u2013 4", 65), ("2", 45), ("1", 25), ("0", 0)],
                ),
                unsafe_allow_html=True,
            )

        with st.container(border=True):
            st.markdown(
                _render_score_card(
                    "Growth", "\U0001F4C8", "#059669",
                    "Are recent videos getting more views than older ones? "
                    "We split the channel\u2019s recent videos into two halves (older and newer) "
                    "and compare their <strong>average views</strong>. "
                    "A positive trend means the audience is growing. Only available in full stats mode.",
                    [("\u2265100%", 100), ("50%", 85), ("20%", 70), ("5%", 55), ("0%", 40), ("\u221220%", 25), ("<\u221250%", 10)],
                ),
                unsafe_allow_html=True,
            )

        with st.container(border=True):
            st.markdown(
                _render_score_card(
                    "Audience Quality", "\U0001F465", "#7C3AED",
                    "How loyal is the audience? We divide <strong>subscribers by total views</strong>. "
                    "A higher ratio means viewers are more likely to subscribe \u2014 "
                    "a sign of genuine audience connection rather than viral flukes.",
                    [("\u226510%", 100), ("5%", 85), ("3%", 70), ("1%", 50), ("0.5%", 35), ("0.1%", 20), ("<0.1%", 10)],
                ),
                unsafe_allow_html=True,
            )

    with th_right:
        with st.container(border=True):
            st.markdown(
                _render_score_card(
                    "Engagement", "\U0001F525", "#3B82F6",
                    "Are people actually interacting with the videos? "
                    "We add up all the <strong>likes + comments</strong> on recent videos "
                    "and divide by total <strong>views</strong>. "
                    "A higher rate means the audience is more active. "
                    "Bigger channels get an easier scale (keeping high engagement at scale is harder).",
                    [("\u226510%", 100), ("7%", 85), ("5%", 70), ("3%", 55), ("1%", 35), ("<1%", 15)],
                ),
                unsafe_allow_html=True,
            )
            if not has_video_stats:
                st.caption("Not available without video stats.")

        with st.container(border=True):
            st.markdown(
                _render_score_card(
                    "Regularity", "\U0001F4C5", "#D97706",
                    "How often does the channel post? "
                    "We count the videos published during your search window "
                    "and divide by the number of weeks to get <strong>posts per week</strong>. "
                    "Channels that post consistently are more reliable partners.",
                    [("4+/wk", 100), ("3/wk", 85), ("2/wk", 70), ("1/wk", 50), ("0.5/wk", 30), ("<0.5/wk", 10)],
                ),
                unsafe_allow_html=True,
            )

        with st.container(border=True):
            st.markdown(
                _render_score_card(
                    "Content Format", "\U0001F3AC", "#EC4899",
                    "What kind of content does the channel make? "
                    "We classify each recent video as a <strong>Short (\u226460s) or long-form</strong>. "
                    "Brands generally prefer long-form creators for deeper integrations. "
                    "Channels with mostly long-form content score higher.",
                    [("\u226490% long", 100), ("70%", 90), ("50%", 70), ("30%", 50), ("10%", 30), ("<10%", 15)],
                ),
                unsafe_allow_html=True,
            )
            if not has_video_stats:
                st.caption("Not available without video stats.")

    st.markdown("")

    # --- Tier system ---
    st.markdown("#### Channel tiers")
    st.markdown(
        '<p class="method-intro">'
        "Channels are grouped into tiers based on how many subscribers they have. "
        "The tier also affects the engagement score \u2014 "
        "a 3% engagement rate is impressive for a channel with millions of followers, "
        "but pretty average for a small one."
        "</p>",
        unsafe_allow_html=True,
    )
    with st.container(border=True):
        for tier_name in TIER_ORDER:
            tc = TIER_COLORS[tier_name]
            boundaries = {
                "mega": "1M+",
                "macro": "100K \u2013 1M",
                "mid": "10K \u2013 100K",
                "micro": "1K \u2013 10K",
                "nano": "< 1K",
            }
            st.markdown(
                f'<div class="tier-row">'
                f'<span class="tier-badge" style="color:{tc["color"]};background:{tc["bg"]};min-width:70px;text-align:center">{tier_name}</span>'
                f'<span class="tier-row-label">{boundaries[tier_name]} subscribers</span>'
                f"</div>",
                unsafe_allow_html=True,
            )
    st.markdown(
        '<p class="tier-note">'
        "\U0001F31F <strong>Emerging</strong> tag \u2014 three paths to qualify (any one is enough):<br>"
        "\u2022 <strong>Growth path:</strong> &lt;100K subs + views trend \u226515% + \u22651 mention + \u22650.3 posts/wk<br>"
        "\u2022 <strong>Fallback path:</strong> &lt;100K subs + \u22652 mentions + \u22650.5 posts/wk (when no trend data)<br>"
        "\u2022 <strong>PAW path:</strong> &lt;100K subs + punch-above-weight ratio \u22653x + \u22651 mention + \u22650.3 posts/wk<br>"
        "Emerging channels get a <strong>+8 bonus</strong> on their global score."
        "</p>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------


def main():
    inject_css()

    # Load API key from config file
    if not st.session_state.get("api_key"):
        stored_key = _load_api_key()
        if stored_key:
            st.session_state["api_key"] = stored_key

    # Fall back to environment variable
    if not st.session_state.get("api_key"):
        env_key = os.environ.get("YOUTUBE_API_KEY", "")
        if env_key:
            st.session_state["api_key"] = env_key

    render_header()
    config = render_search_config()

    # Run search if button clicked
    if config["run_btn"]:
        run_search(config)

    # Channel lookup tab
    with st.expander("🔗 Channel Lookup — score specific channels by URL", expanded=False):
        st.caption(
            "Paste YouTube channel URLs or @handles (one per line) to score them directly — "
            "useful for creators that don't appear in keyword searches."
        )
        lookup_urls = st.text_area(
            "Channel URLs / @handles",
            placeholder="https://www.youtube.com/@THEPAF\nhttps://www.youtube.com/@example\n@anothercreator",
            height=120,
            label_visibility="collapsed",
        )
        lookup_btn = st.button("Score these channels", type="secondary")

        if lookup_btn and lookup_urls.strip():
            api_key = st.session_state.get("api_key", "")
            if not api_key:
                st.error("API key missing — add it in Settings.")
            else:
                urls = [u.strip() for u in lookup_urls.strip().splitlines() if u.strip()]
                with st.status(f"Scoring {len(urls)} channel(s)...", expanded=True) as lookup_status:
                    try:
                        youtube = get_youtube_client(api_key)
                        st.write("Resolving channel URLs...")
                        channel_ids = resolve_channel_urls(youtube, urls)
                        if not channel_ids:
                            lookup_status.update(label="No valid channels found", state="error")
                            st.warning("Could not resolve any channel from the provided URLs.")
                        else:
                            st.write(f"Fetching details for {len(channel_ids)} channel(s)...")
                            channel_details = get_channel_details(youtube, channel_ids)
                            st.write("Computing scores...")
                            collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            profiles = []
                            for cid in channel_ids:
                                details = channel_details.get(cid, {})
                                if not details:
                                    continue
                                search_data = {"channel_id": cid, "display_name": details.get("display_name", ""), "video_ids": [], "mentions_count": 0}
                                try:
                                    vstats = get_recent_video_stats(youtube, cid, config["days"])
                                    time.sleep(RATE_LIMIT_VIDEO_STATS)
                                    has_stats = True
                                except Exception:
                                    vstats = dict(ZERO_VIDEO_STATS)
                                    has_stats = False
                                metrics = compute_channel_metrics(details, vstats, search_data, config["days"])
                                profile = build_channel_profile(cid, details, search_data, metrics, has_stats, collected_at, config["region"])
                                profiles.append(profile)

                            if profiles:
                                # Merge with existing results if any
                                existing = st.session_state.get("profiles", [])
                                existing_ids = {p.get("profile_url") for p in existing}
                                new_profiles = [p for p in profiles if p.get("profile_url") not in existing_ids]
                                merged = existing + new_profiles
                                st.session_state["profiles"] = merged
                                st.session_state["df"] = (
                                    pd.DataFrame(merged, columns=COLUMNS)
                                    .sort_values("score_global", ascending=False)
                                    .reset_index(drop=True)
                                )
                                st.session_state["has_video_stats"] = True
                                lookup_status.update(label=f"Done — {len(profiles)} channel(s) scored", state="complete")
                                st.rerun()
                            else:
                                lookup_status.update(label="No profiles built", state="error")
                    except Exception as e:
                        lookup_status.update(label="Error", state="error")
                        st.error(f"Error: {e}")

    # Show onboarding or results
    if "df" not in st.session_state or st.session_state["df"] is None:
        render_empty_state()
        return

    df = st.session_state["df"]
    has_video_stats = st.session_state.get("has_video_stats", False)

    # Summary strip
    render_summary_strip(df)

    # Creator list with filters
    render_creator_list(df, has_video_stats)

    # Methodology expander
    with st.expander("Methodology & Scoring"):
        render_methodology(has_video_stats)


main()
