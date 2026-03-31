"""
YouTube Creator Scraper — Streamlit Web UI
Launch: streamlit run app.py
"""

import io
import json
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
    RATE_LIMIT_VIDEO_STATS,
    SCORE_WEIGHTS,
    ZERO_VIDEO_STATS,
    build_channel_profile,
    clear_cache,
    compute_channel_metrics,
    export_csv,
    export_excel,
    export_json,
    get_channel_details,
    get_recent_video_stats,
    get_video_stats_batch,
    get_youtube_client,
    merge_keyword_results,
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

LANGUAGE_OPTIONS = ["All languages", "fr", "en", "de", "es", "it", "pt"]

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
    "sorare_mentions": "Keyword Mentions",
    "score_pertinence": "Relevance",
    "score_engagement": "Engagement",
    "score_croissance": "Growth",
    "score_regularite": "Regularity",
    "croissance_hebdo": "Weekly Growth",
    "engagement_rate_pct": "Engagement %",
    "posts_per_week": "Posts/week",
    "score_global": "Global Score",
    "followers": "Followers",
    "tier": "Tier",
    "display_name": "Channel",
    "is_emerging": "Emerging",
    "status": "Status",
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

    /* --- Onboarding --- */
    .onboarding-step {
        text-align: center; padding: 20px 12px;
    }
    .onboarding-step .step-num {
        display: inline-block; width: 32px; height: 32px; line-height: 32px;
        border-radius: 50%; background: #000; color: #fff;
        font-weight: 700; font-size: 14px; margin-bottom: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .onboarding-step .step-title {
        font-size: 15px; font-weight: 600; color: #0F172A; margin-bottom: 4px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .onboarding-step .step-desc {
        font-size: 13px; color: #64748B;
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
    st.caption("API responses are cached locally to save quota. Search results: 4h, channel details: 24h, video stats: 4h.")
    if st.button("Clear Cache", use_container_width=True):
        clear_cache()
        st.success("Cache cleared.")


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
            label = "API Connected" if has_key else "No API Key"
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
        # Row 1: Keywords | Region | Language | Period
        col_kw, col_region, col_lang, col_period = st.columns([4, 2, 2, 2])

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
                help="Filter results by country. Leave on Worldwide for global search.",
            )

        with col_lang:
            language = st.selectbox(
                "Language",
                options=LANGUAGE_OPTIONS,
                index=0,
                help="Filter results by relevance language.",
            )

        with col_period:
            days = st.slider(
                "Period (days)",
                min_value=7,
                max_value=365,
                value=90,
                step=7,
                help="Time window for video publication analysis.",
            )

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
                "Max ch/keyword",
                min_value=10,
                max_value=300,
                value=100,
                step=10,
            )

        with r2_stats:
            stats_mode = st.selectbox(
                "Video stats mode",
                options=["Fast", "Full", "None"],
                index=0,
                help="Fast: reuse search video IDs (~1 unit/batch). Full: per-channel search (~100 units/ch). None: skip.",
            )
            kw_count = len([k for k in keywords_raw.split(",") if k.strip()])
            quota_est = max_channels * 100 + kw_count * 300
            if stats_mode == "Full":
                quota_est += max_channels * 100
            elif stats_mode == "Fast":
                quota_est += max_channels  # ~1 unit per batch of 50
            st.caption(f"Est. quota: ~{quota_est:,} / 10K")

        with r2_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            run_btn = st.button("Search", use_container_width=True, type="primary")

            # Download button (only when results exist)
            if st.session_state.get("profiles"):
                dl_format = st.selectbox("Format", ["Excel", "CSV", "JSON"], key="dl_format", label_visibility="collapsed")
                profiles = st.session_state["profiles"]
                keywords = st.session_state.get("search_keywords", [])
                base_name = datetime.now().strftime("%Y%m%d")

                if dl_format == "Excel":
                    buf = io.BytesIO()
                    export_excel(profiles, buf, keywords)
                    buf.seek(0)
                    st.download_button(label="Download", data=buf, file_name=f"youtube_{base_name}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                elif dl_format == "CSV":
                    buf = io.StringIO()
                    export_csv(profiles, buf, keywords)
                    st.download_button(label="Download", data=buf.getvalue(), file_name=f"youtube_{base_name}.csv", mime="text/csv", use_container_width=True)
                else:  # JSON
                    buf = io.BytesIO()
                    export_json(profiles, buf, keywords)
                    buf.seek(0)
                    st.download_button(label="Download", data=buf, file_name=f"youtube_{base_name}.json", mime="application/json", use_container_width=True)

    return {
        "run_btn": run_btn,
        "keywords_raw": keywords_raw,
        "region": REGION_OPTIONS[region_label],
        "days": days,
        "api_key": st.session_state.get("api_key", ""),
        "language": None if language == "All languages" else language,
        "followers_min": followers_min,
        "followers_max": followers_max,
        "max_channels": max_channels,
        "stats_mode": stats_mode.lower(),  # "fast", "full", or "none"
        "output_name": f"youtube_{datetime.now().strftime('%Y%m%d')}.xlsx",
    }


# ---------------------------------------------------------------------------
# Empty state / onboarding
# ---------------------------------------------------------------------------


def render_onboarding():
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(
        '<div style="text-align:center;margin:40px 0 16px 0">'
        '<p style="font-size:28px;font-weight:700;color:#0F172A;margin-bottom:4px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">'
        "Find and score YouTube creators</p>"
        '<p style="font-size:16px;color:#64748B;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">'
        "Search by keywords, filter by region and followers, and export ranked results.</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            '<div class="onboarding-step">'
            '<div class="step-num">1</div>'
            '<div class="step-title">Enter keywords</div>'
            '<div class="step-desc">Add one or more comma-separated search terms to find relevant creators.</div>'
            "</div>",
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            '<div class="onboarding-step">'
            '<div class="step-num">2</div>'
            '<div class="step-title">Review results</div>'
            '<div class="step-desc">Browse scored channels with engagement breakdown, growth, and contact info.</div>'
            "</div>",
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            '<div class="onboarding-step">'
            '<div class="step-num">3</div>'
            '<div class="step-title">Export shortlist</div>'
            '<div class="step-desc">Download a styled Excel report to share with your team.</div>'
            "</div>",
            unsafe_allow_html=True,
        )

    # Quick-start examples
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<p style="text-align:center;font-size:14px;color:#64748B;font-weight:500;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">'
        "Quick start examples</p>",
        unsafe_allow_html=True,
    )
    qc1, qc2, qc3 = st.columns(3)
    with qc1:
        if st.button("Sorare (France, 90 days)", use_container_width=True):
            st.session_state["keywords_raw"] = "Sorare"
            st.rerun()
    with qc2:
        if st.button("NFT Gaming (Worldwide)", use_container_width=True):
            st.session_state["keywords_raw"] = "NFT Gaming"
            st.rerun()
    with qc3:
        if st.button("Fantasy Football", use_container_width=True):
            st.session_state["keywords_raw"] = "Fantasy Football"
            st.rerun()


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

                has_stats = mode != "none"
                metrics = compute_channel_metrics(details, vstats, search_data, config["days"])
                profile = build_channel_profile(cid, details, search_data, metrics, has_stats, collected_at)
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
        st.markdown(
            f'<div class="detail-metric"><div class="dm-value">{row.get("growth_rate_pct", 0):.2f}%</div><div class="dm-label">Growth</div></div>',
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
        with_mentions = int((df["sorare_mentions"] > 0).sum())
        st.markdown(kpi_card(str(with_mentions), "With Mentions"), unsafe_allow_html=True)
    with kc6:
        emerging_count = int(df["is_emerging"].sum())
        st.markdown(kpi_card(str(emerging_count), "Emerging"), unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Creator list
# ---------------------------------------------------------------------------


def render_creator_list(df: pd.DataFrame, has_video_stats: bool):
    st.markdown("<br>", unsafe_allow_html=True)

    # Filter bar
    fc1, fc2, fc3, fc4 = st.columns([2, 2, 1, 3])
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
        filtered = filtered[filtered["display_name"].str.contains(name_search, case=False, na=False)]

    st.caption(f"{len(filtered)} channels displayed")

    if filtered.empty:
        st.info("No channels match your filters.")
        return

    # Display columns — includes engagement breakdown
    display_cols = [
        "display_name",
        "tier",
        "followers",
        "score_global",
        "engagement_rate_pct",
        "total_recent_views",
        "total_recent_likes",
        "total_recent_comments",
        "growth_rate_pct",
        "posts_per_week",
        "email",
        "profile_url",
    ]

    col_config = {
        "display_name": st.column_config.TextColumn("Channel", width="medium"),
        "tier": st.column_config.TextColumn("Tier", width="small"),
        "followers": st.column_config.NumberColumn("Followers", format="%d"),
        "score_global": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.0f"),
        "engagement_rate_pct": st.column_config.NumberColumn("Eng. %", format="%.2f"),
        "total_recent_views": st.column_config.NumberColumn("Views", format="%d"),
        "total_recent_likes": st.column_config.NumberColumn("Likes", format="%d"),
        "total_recent_comments": st.column_config.NumberColumn("Comments", format="%d"),
        "growth_rate_pct": st.column_config.NumberColumn("Growth %/wk", format="%.2f"),
        "posts_per_week": st.column_config.NumberColumn("Posts/wk", format="%.1f"),
        "email": st.column_config.TextColumn("Email", width="medium"),
        "profile_url": st.column_config.LinkColumn("YouTube", width="small", display_text="Link"),
    }

    # Interactive table with row selection
    event = st.dataframe(
        filtered[display_cols],
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
    st.markdown("#### Scoring Formula")

    w = SCORE_WEIGHTS
    if has_video_stats:
        weights = {
            "Relevance": w["pertinence"],
            "Engagement": w["engagement"],
            "Growth": w["croissance"],
            "Regularity": w["regularite"],
        }
    else:
        non_engagement = w["pertinence"] + w["croissance"] + w["regularite"]
        weights = {
            "Relevance": round(w["pertinence"] / non_engagement, 2),
            "Growth": round(w["croissance"] / non_engagement, 2),
            "Regularity": round(w["regularite"] / non_engagement, 2),
        }

    weight_colors = {
        "Relevance": "#000000",
        "Engagement": "#3B82F6",
        "Growth": "#10B981",
        "Regularity": "#F59E0B",
    }

    for name, val in weights.items():
        pct = int(val * 100)
        color = weight_colors.get(name, "#64748B")
        st.markdown(
            f'<div class="weight-bar">'
            f'<span class="wb-label">{name}</span>'
            f'<div class="wb-bar" style="width:{pct * 3}px;background:{color}"></div>'
            f'<span class="wb-pct">{pct}%</span>'
            f"</div>",
            unsafe_allow_html=True,
        )

    if not has_video_stats:
        st.info("Video stats disabled. Engagement is excluded and weights are renormalized.")

    st.markdown("---")

    # Threshold tables
    st.markdown("#### Score Thresholds")
    th_left, th_right = st.columns(2)

    with th_left:
        with st.container(border=True):
            st.markdown("**Relevance** (keyword mentions in video titles)")
            st.markdown("""
| Mentions | Score |
|----------|-------|
| 10+ | 100 |
| 5-9 | 85 |
| 3-4 | 65 |
| 2 | 45 |
| 1 | 25 |
| 0 | 0 |
            """)

        with st.container(border=True):
            st.markdown("**Growth** (weekly follower growth rate)")
            st.markdown("""
| Growth %/week | Score |
|---------------|-------|
| 30%+ | 100 |
| 20% | 85 |
| 10% | 65 |
| 5% | 45 |
| 1% | 25 |
| <1% | 10 |
            """)

    with th_right:
        with st.container(border=True):
            st.markdown("**Engagement** (likes + comments / views)")
            st.markdown("""
| Engagement Rate | Score |
|-----------------|-------|
| 10%+ | 100 |
| 7% | 85 |
| 5% | 70 |
| 3% | 55 |
| 1% | 35 |
| <1% | 15 |
            """)
            if not has_video_stats:
                st.caption("Not available without video stats.")

        with st.container(border=True):
            st.markdown("**Regularity** (posting frequency)")
            st.markdown("""
| Posts/week | Score |
|------------|-------|
| 4+ | 100 |
| 3 | 85 |
| 2 | 70 |
| 1 | 50 |
| 0.5 | 30 |
| <0.5 | 10 |
            """)

    st.markdown("---")

    # Tier system
    st.markdown("#### Tier System")
    with st.container(border=True):
        for tier_name in TIER_ORDER:
            tc = TIER_COLORS[tier_name]
            boundaries = {"mega": "1M+", "macro": "100K - 1M", "mid": "10K - 100K", "micro": "1K - 10K", "nano": "< 1K"}
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:12px;padding:6px 0">'
                f'<span class="tier-badge" style="color:{tc["color"]};background:{tc["bg"]};min-width:70px;text-align:center">{tier_name}</span>'
                f'<span style="font-size:14px;color:#0F172A;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">{boundaries[tier_name]} followers</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
    st.caption("**Emerging** = weekly growth > 5% AND fewer than 50K followers.")


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

    # Show onboarding or results
    if "df" not in st.session_state or st.session_state["df"] is None:
        render_onboarding()
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
