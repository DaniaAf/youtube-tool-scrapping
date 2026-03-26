"""
YouTube Profile Scraper — Interface Web (Streamlit)
Lancer : streamlit run app.py
"""

import io
import os
import time
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Import du scraper (mêmes fonctions que youtube_scraper.py)
from youtube_scraper import (
    get_youtube_client,
    search_videos_by_keyword,
    get_channel_details,
    get_recent_video_stats,
    calculate_tier,
    compute_scores,
    export_excel,
    COLUMNS,
)
from googleapiclient.errors import HttpError

# ---------------------------------------------------------------------------
# Config page
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Sorare YouTube Scraper",
    page_icon="⭐",
    layout="wide",
    initial_sidebar_state="expanded",
)

LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "Sorare26-Logo-Black.png")

st.markdown("""
<style>
    /* Progress bar */
    .stProgress > div > div { background-color: #000000; }
    /* Bouton lancer */
    [data-testid="stSidebar"] .stButton > button {
        background: #000000 !important;
        color: #ffffff !important;
        font-weight: 800 !important;
        border: none !important;
        border-radius: 6px !important;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: #333333 !important;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar — paramètres
# ---------------------------------------------------------------------------

with st.sidebar:
    st.image(LOGO_PATH, width=200)
    st.markdown('<div style="color:#888; font-size:12px; margin-bottom:12px; font-family:Arial">YouTube Creator Scraper</div>', unsafe_allow_html=True)
    st.markdown("---")

    api_key = st.text_input(
        "Clé API YouTube Data v3",
        value=os.environ.get("YOUTUBE_API_KEY", ""),
        type="password",
        help="Obtenable sur console.cloud.google.com",
    )

    st.markdown("### Critères de recherche")

    keywords_raw = st.text_area(
        "Mots-clés (un par ligne)",
        value="Sorare",
        height=100,
        help="Chaque mot-clé génère une recherche distincte. Les résultats sont fusionnés.",
    )

    region_options = {
        "🌍 Monde entier": None,
        "🇫🇷 France (FR)": "FR",
        "🇧🇪 Belgique (BE)": "BE",
        "🇨🇭 Suisse (CH)": "CH",
        "🇬🇧 Royaume-Uni (GB)": "GB",
        "🇺🇸 États-Unis (US)": "US",
        "🇩🇪 Allemagne (DE)": "DE",
        "🇪🇸 Espagne (ES)": "ES",
        "🇮🇹 Italie (IT)": "IT",
        "🇧🇷 Brésil (BR)": "BR",
        "🇨🇦 Canada (CA)": "CA",
    }
    region_label = st.selectbox(
        "Pays / Région",
        options=list(region_options.keys()),
        index=0,
        help="Optionnel — laisser sur Monde entier pour ne pas filtrer par pays",
    )
    region = region_options[region_label]

    language = st.selectbox(
        "Langue de pertinence",
        options=["(aucune)", "fr", "en", "de", "es", "it", "pt"],
        index=0,
        help="Optionnel — filtre la pertinence des résultats par langue",
    )

    days = st.slider(
        "Période (jours)",
        min_value=7,
        max_value=365,
        value=90,
        step=7,
        help="Optionnel — fenêtre de publication des vidéos analysées (défaut : 90 jours)",
    )

    st.markdown("**Abonnés**")
    col_min, col_max = st.columns(2)
    with col_min:
        followers_min = st.number_input(
            "Min", min_value=0, value=0, step=1000,
            help="Optionnel — laisser à 0 pour ignorer",
        )
    with col_max:
        followers_max = st.number_input(
            "Max", min_value=0, value=0, step=10000,
            help="Optionnel — laisser à 0 pour ignorer",
        )

    max_channels = st.slider(
        "Max chaînes par mot-clé",
        min_value=10,
        max_value=300,
        value=100,
        step=10,
        help="Optionnel",
    )

    fetch_stats = st.toggle(
        "Récupérer les stats vidéo détaillées",
        value=False,
        help="⚠️ Coûteux en quota : 100 unités × nombre de chaînes. À activer uniquement si quota suffisant.",
    )
    quota_cost = max_channels * 100 + len([k for k in keywords_raw.strip().splitlines() if k.strip()]) * 300
    if fetch_stats:
        st.caption(f"⚠️ Coût estimé : ~{quota_cost:,} unités (quota = 10 000/jour)")

    output_name = st.text_input(
        "Nom du fichier Excel",
        value=f"youtube_{datetime.now().strftime('%Y%m%d')}.xlsx",
        help="Optionnel",
    )

    st.markdown("---")
    run_btn = st.button("🚀 Lancer la recherche", use_container_width=True, type="primary")

# ---------------------------------------------------------------------------
# Zone principale
# ---------------------------------------------------------------------------

st.image(LOGO_PATH, width=340)
st.markdown(
    '<p style="color:#555;font-size:15px;margin-top:-8px;font-family:Arial,sans-serif">'
    'YouTube Creator Scraper — identifie les créateurs par mots-clés, pays et période'
    '</p>',
    unsafe_allow_html=True,
)
st.markdown("---")

if not run_btn:
    st.info("👈 Configure tes critères dans la barre latérale puis clique sur **Lancer la recherche**.")
    st.markdown("""
    ### Colonnes exportées
    | Colonne | Description |
    |---|---|
    | `followers` | Abonnés de la chaîne |
    | `tier` | nano / micro / mid / macro / mega |
    | `engagement_rate_pct` | (likes + commentaires) / vues × 100 |
    | `sorare_mentions` | Fois où le mot-clé apparaît dans titres/descriptions |
    | `score_global` | Score pondéré 0–100 |
    | `score_pertinence` | Pertinence du mot-clé (37 %) |
    | `score_engagement` | Taux d'engagement (28 %) |
    | `score_croissance` | Croissance estimée (20 %) |
    | `score_regularite` | Fréquence de publication (15 %) |
    | `is_emerging` | Chaîne en forte croissance < 50K abonnés |
    """)
    st.stop()

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

keywords = [k.strip() for k in keywords_raw.strip().splitlines() if k.strip()]

if not api_key:
    st.error("Clé API manquante — ajoute-la dans la barre latérale ou dans ton fichier `.env`.")
    st.stop()

if not keywords:
    st.error("Ajoute au moins un mot-clé.")
    st.stop()

lang = None if language == "(aucune)" else language

# ---------------------------------------------------------------------------
# Scraping avec barre de progression
# ---------------------------------------------------------------------------

st.markdown("---")
status = st.empty()
progress_bar = st.progress(0)
log_box = st.empty()

profiles = []
logs = []

def log(msg):
    logs.append(msg)
    log_box.markdown("\n".join(f"- {l}" for l in logs[-8:]))

try:
    youtube = get_youtube_client(api_key)

    # --- Étape 1 : recherche par mot-clé ---
    all_channels = {}
    for i, kw in enumerate(keywords):
        status.info(f"🔍 Recherche mot-clé **{kw}** ({i+1}/{len(keywords)})…")
        log(f"Recherche « {kw} » | région={region or 'monde entier'} | {days} jours")
        found = search_videos_by_keyword(youtube, kw, region, days, lang, max_channels)
        log(f"→ {len(found)} chaînes trouvées pour « {kw} »")

        for cid, data in found.items():
            if cid not in all_channels:
                all_channels[cid] = data
            else:
                all_channels[cid]["mentions_count"] += data["mentions_count"]
                all_channels[cid]["video_ids"].extend(data["video_ids"])

        progress_bar.progress(int((i + 1) / len(keywords) * 30))

    if not all_channels:
        st.warning("Aucune chaîne trouvée. Essaie d'autres mots-clés, une période plus longue, ou vérifie ta clé API.")
        st.stop()
    log(f"Total unique : {len(all_channels)} chaînes")

    channel_ids = list(all_channels.keys())[:max_channels]
    status.info(f"📋 Récupération des détails pour {len(channel_ids)} chaînes…")
    log(f"Récupération détails — {len(channel_ids)} chaînes")

    channel_details = get_channel_details(youtube, channel_ids)
    progress_bar.progress(50)

    # --- Étape 2 : stats vidéo + calcul métriques ---
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for idx, cid in enumerate(channel_ids):
        details = channel_details.get(cid, {})
        search_data = all_channels[cid]
        followers = details.get("followers", 0)

        if fetch_stats:
            try:
                vstats = get_recent_video_stats(youtube, cid, days)
                time.sleep(0.1)
            except HttpError:
                vstats = {"views": 0, "likes": 0, "comments": 0, "video_count": 0}
        else:
            vstats = {"views": 0, "likes": 0, "comments": 0, "video_count": 0}

        video_count = vstats["video_count"]
        total_views = vstats["views"]
        total_likes = vstats["likes"]
        total_comments = vstats["comments"]

        engagement_rate = (
            (total_likes + total_comments) / total_views if total_views > 0 else 0.0
        )
        weeks = days / 7
        posts_per_week = round(video_count / weeks, 2) if weeks > 0 else 0

        published_at_str = details.get("published_at", "")
        if published_at_str:
            try:
                from datetime import timezone
                created = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                age_weeks = max((datetime.now(timezone.utc) - created).days / 7, 1)
                croissance_hebdo = round(followers / age_weeks, 1)
                growth_rate_pct = round((croissance_hebdo / max(followers, 1)) * 100, 2)
            except ValueError:
                croissance_hebdo = 0.0
                growth_rate_pct = 0.0
        else:
            croissance_hebdo = 0.0
            growth_rate_pct = 0.0

        mentions = search_data.get("mentions_count", 0)
        is_emerging = growth_rate_pct > 5 and followers < 50_000

        se, sc, sp, sr, sg = compute_scores(engagement_rate, mentions, posts_per_week, growth_rate_pct, has_video_stats=fetch_stats)

        username = details.get("username") or cid
        if details.get("username"):
            profile_url = f"https://www.youtube.com/@{details['username'].lstrip('@')}"
        else:
            profile_url = f"https://www.youtube.com/channel/{cid}"

        # Filtre abonnés
        if followers_min > 0 and followers < followers_min:
            continue
        if followers_max > 0 and followers > followers_max:
            continue

        profiles.append({
            "platform": "YouTube",
            "username": username,
            "display_name": details.get("display_name") or search_data.get("display_name", ""),
            "profile_url": profile_url,
            "bio_snippet": details.get("bio_snippet", ""),
            "followers": followers,
            "tier": calculate_tier(followers),
            "engagement_rate": round(engagement_rate, 6),
            "engagement_rate_pct": round(engagement_rate * 100, 3),
            "croissance_hebdo": croissance_hebdo,
            "growth_rate_pct": growth_rate_pct,
            "posts_per_week": posts_per_week,
            "sorare_mentions": mentions,
            "is_emerging": is_emerging,
            "score_global": sg,
            "score_engagement": se,
            "score_croissance": sc,
            "score_pertinence": sp,
            "score_regularite": sr,
            "status": "active" if posts_per_week >= 0.5 else "inactive",
            "collected_at": collected_at,
        })

        pct = 50 + int((idx + 1) / len(channel_ids) * 45)
        progress_bar.progress(pct)

        if (idx + 1) % 10 == 0:
            log(f"Traitement : {idx + 1}/{len(channel_ids)} chaînes…")

    progress_bar.progress(100)
    status.success(f"✅ {len(profiles)} profils collectés !")
    log("Terminé.")

except HttpError as e:
    err_str = str(e)
    if "quotaExceeded" in err_str or "rateLimitExceeded" in err_str:
        st.error("❌ Quota journalier YouTube épuisé (10 000 unités/jour).")
        st.info("💡 Solutions : attends minuit heure du Pacifique (~9h CET), désactive les **stats vidéo détaillées**, ou réduis le nombre de chaînes max.")
    elif "keyInvalid" in err_str or "API key not valid" in err_str:
        st.error("❌ Clé API invalide. Vérifie que la clé est bien copiée dans `.env`.")
    elif "forbidden" in err_str.lower():
        st.error("❌ Accès refusé. Vérifie que l'API YouTube Data v3 est bien activée dans Google Console.")
    else:
        st.error(f"❌ Erreur API YouTube : {e}")
    st.stop()
except Exception as e:
    st.error(f"❌ Erreur inattendue : {e}")
    st.stop()

# ---------------------------------------------------------------------------
# Résultats
# ---------------------------------------------------------------------------

if not profiles:
    st.warning("Aucun profil à afficher.")
    st.stop()

df = pd.DataFrame(profiles, columns=COLUMNS)
df.sort_values("score_global", ascending=False, inplace=True)
df_display = df.reset_index(drop=True)

st.markdown("---")
st.markdown("### Résultats de la recherche")

# Métriques résumé
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Chaînes trouvées", len(df))
c2.metric("Score moyen", f"{df['score_global'].mean():.1f} / 100")
c3.metric("Avg engagement", f"{df['engagement_rate_pct'].mean():.2f} %")
c4.metric("Avec mentions", int((df["sorare_mentions"] > 0).sum()))
c5.metric("Emerging", int((df["is_emerging"] == True).sum()))

st.markdown("#### Répartition par tier")
tier_counts = df["tier"].value_counts().reindex(["mega", "macro", "mid", "micro", "nano"], fill_value=0)
st.bar_chart(tier_counts)

# Tableau interactif
st.markdown("#### Tableau des profils (trié par score global)")

display_cols = [
    "display_name", "profile_url", "username", "tier", "followers",
    "engagement_rate_pct", "posts_per_week", "sorare_mentions",
    "score_global", "score_pertinence", "score_engagement",
    "score_croissance", "score_regularite", "is_emerging", "status",
]

st.dataframe(
    df_display[display_cols],
    use_container_width=True,
    height=420,
    column_config={
        "display_name": st.column_config.TextColumn("Chaîne", width="medium"),
        "profile_url": st.column_config.LinkColumn("🔗 Lien", display_text="▶ Voir", width="small"),
        "username": st.column_config.TextColumn("Handle"),
        "tier": st.column_config.TextColumn("Tier"),
        "followers": st.column_config.NumberColumn("Abonnés", format="%d"),
        "engagement_rate_pct": st.column_config.NumberColumn("Engagement %", format="%.2f"),
        "posts_per_week": st.column_config.NumberColumn("Posts/sem", format="%.1f"),
        "sorare_mentions": st.column_config.NumberColumn("Mentions"),
        "score_global": st.column_config.ProgressColumn("Score global", min_value=0, max_value=100, format="%.0f"),
        "score_pertinence": st.column_config.ProgressColumn("Pertinence", min_value=0, max_value=100, format="%.0f"),
        "score_engagement": st.column_config.ProgressColumn("Engagement" + ("" if fetch_stats else " (N/A)"), min_value=0, max_value=100, format="%.0f"),
        "score_croissance": st.column_config.ProgressColumn("Croissance", min_value=0, max_value=100, format="%.0f"),
        "score_regularite": st.column_config.ProgressColumn("Régularité", min_value=0, max_value=100, format="%.0f"),
        "is_emerging": st.column_config.CheckboxColumn("Emerging"),
        "status": st.column_config.TextColumn("Statut"),
    },
    hide_index=True,
)

# ---------------------------------------------------------------------------
# Explication du scoring
# ---------------------------------------------------------------------------

with st.expander("📊 Comment est calculé le scoring ?", expanded=False):

    st.markdown("Le **score global** est une note sur **0 à 100** qui combine plusieurs composantes, chacune pondérée selon son importance.")

    # --- Formule visuelle ---
    st.markdown("---")
    st.markdown("### Formule du score global")

    if fetch_stats:
        st.markdown("""
<div style="background:#f0f4ff;border-radius:10px;padding:16px 20px;font-size:15px;line-height:2">
<b>Score global</b> =
&nbsp;&nbsp;🎯 Pertinence &nbsp;<span style="background:#000;color:#fff;border-radius:4px;padding:2px 7px;font-size:13px">× 37%</span>
&nbsp;+&nbsp; 💬 Engagement &nbsp;<span style="background:#1d4ed8;color:#fff;border-radius:4px;padding:2px 7px;font-size:13px">× 28%</span>
&nbsp;+&nbsp; 📈 Croissance &nbsp;<span style="background:#059669;color:#fff;border-radius:4px;padding:2px 7px;font-size:13px">× 20%</span>
&nbsp;+&nbsp; 🗓️ Régularité &nbsp;<span style="background:#d97706;color:#fff;border-radius:4px;padding:2px 7px;font-size:13px">× 15%</span>
</div>
        """, unsafe_allow_html=True)
        st.caption("Exemple : Pertinence=85, Engagement=70, Croissance=45, Régularité=50 → Score = 85×0.37 + 70×0.28 + 45×0.20 + 50×0.15 = **31.45 + 19.6 + 9 + 7.5 = 67.5 / 100**")
    else:
        st.markdown("""
<div style="background:#f0f4ff;border-radius:10px;padding:16px 20px;font-size:15px;line-height:2">
<b>Score global</b> (sans stats vidéo) =
&nbsp;&nbsp;🎯 Pertinence &nbsp;<span style="background:#000;color:#fff;border-radius:4px;padding:2px 7px;font-size:13px">× 51%</span>
&nbsp;+&nbsp; 📈 Croissance &nbsp;<span style="background:#059669;color:#fff;border-radius:4px;padding:2px 7px;font-size:13px">× 28%</span>
&nbsp;+&nbsp; 🗓️ Régularité &nbsp;<span style="background:#d97706;color:#fff;border-radius:4px;padding:2px 7px;font-size:13px">× 21%</span>
</div>
        """, unsafe_allow_html=True)
        st.caption("Les poids sont renormalisés sur 100% car l'engagement n'est pas disponible sans stats vidéo. Exemple : Pertinence=85, Croissance=45, Régularité=10 → Score = 85×0.51 + 45×0.28 + 10×0.21 = **43.35 + 12.6 + 2.1 = 58 / 100**")

    st.markdown("---")
    st.markdown("### Détail de chaque composante")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🎯 Pertinence — 37%")
        st.caption("Combien de vidéos de cette chaîne mentionnent le mot-clé dans leur titre ? Plus une chaîne parle du sujet, plus elle est pertinente.")
        st.markdown("""
| Vidéos avec le mot-clé | Score composante |
|---|---|
| 10+ vidéos | **100** → apporte **37 pts** au score global |
| 5–9 vidéos | **85** → apporte **31 pts** |
| 3–4 vidéos | **65** → apporte **24 pts** |
| 2 vidéos | **45** → apporte **17 pts** |
| 1 vidéo | **25** → apporte **9 pts** |
| 0 vidéo | **0** → apporte **0 pt** |
        """)

        st.markdown("#### 📈 Croissance — 20%")
        st.caption("Vitesse de croissance moyenne de la chaîne depuis sa création : abonnés ÷ âge en semaines. Favorise les chaînes en forte montée.")
        st.markdown("""
| Croissance hebdo estimée | Score composante |
|---|---|
| +30% / semaine | **100** → apporte **20 pts** |
| +20% | **85** → apporte **17 pts** |
| +10% | **65** → apporte **13 pts** |
| +5% | **45** → apporte **9 pts** |
| +1% | **25** → apporte **5 pts** |
| < 1% | **10** → apporte **2 pts** |
        """)
        st.caption("⚠️ C'est une croissance moyenne depuis la création, pas la croissance récente réelle.")

    with col2:
        st.markdown("#### 💬 Engagement — 28%")
        st.caption("(Likes + commentaires) ÷ vues sur les vidéos récentes. Mesure la qualité de l'interaction avec la communauté.")
        st.markdown("""
| Taux d'engagement | Score composante |
|---|---|
| 10%+ | **100** → apporte **28 pts** |
| 7% | **85** → apporte **24 pts** |
| 5% | **70** → apporte **20 pts** |
| 3% | **55** → apporte **15 pts** |
| 1% | **35** → apporte **10 pts** |
| < 1% | **15** → apporte **4 pts** |
        """)
        if not fetch_stats:
            st.warning("⚠️ Stats vidéo OFF — engagement non calculé, retiré de la formule.")
        else:
            st.caption("Nécessite stats vidéo activées.")

        st.markdown("#### 🗓️ Régularité — 15%")
        st.caption("Fréquence de publication sur la période analysée. Un créateur régulier est plus fiable pour un partenariat.")
        st.markdown("""
| Posts / semaine | Score composante |
|---|---|
| 4+ | **100** → apporte **15 pts** |
| 3 | **85** → apporte **13 pts** |
| 2 | **70** → apporte **10 pts** |
| 1 | **50** → apporte **7 pts** |
| 0.5 | **30** → apporte **4 pts** |
| < 0.5 | **10** → apporte **1 pt** |
        """)
        if not fetch_stats:
            st.warning("⚠️ Stats vidéo OFF — régularité non calculée.")
        else:
            st.caption("Nécessite stats vidéo activées.")

    st.markdown("---")
    st.markdown("### Lecture rapide du score global")
    st.markdown("""
| Score global | Interprétation |
|---|---|
| **80 – 100** | Créateur très pertinent, forte présence sur le sujet |
| **60 – 79** | Bon profil, à contacter en priorité |
| **40 – 59** | Profil intéressant, pertinence moyenne |
| **20 – 39** | Présence faible sur le sujet, à cibler en dernier recours |
| **< 20** | Mention anecdotique, peu pertinent |
    """)

    st.markdown("---")
    st.markdown("""
**🏷️ Tiers (tranches d'abonnés)**

| Tier | Abonnés | Profil type |
|---|---|---|
| 🟣 Mega | 1 000 000+ | Star YouTube, reach massif |
| 🔵 Macro | 100 000 – 999 999 | Créateur établi |
| 🟢 Mid | 10 000 – 99 999 | Communauté engagée |
| 🟡 Micro | 1 000 – 9 999 | Niche, fort engagement |
| ⚫ Nano | < 1 000 | Débutant ou très niche |

**⚡ Is Emerging** = croissance > 5% / semaine **ET** moins de 50 000 abonnés → chaîne en forte montée à surveiller
    """)

# ---------------------------------------------------------------------------
# Export Excel
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("### Télécharger le fichier Excel")

buf = io.BytesIO()
export_excel(profiles, buf, keywords)  # type: ignore[arg-type]
buf.seek(0)

st.download_button(
    label="⬇️ Télécharger le fichier Excel",
    data=buf,
    file_name=output_name,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
    type="primary",
)
