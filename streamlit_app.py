import unicodedata
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st

import olx_scraper_background as app


st.set_page_config(page_title="OLX Piasek - UI", layout="wide")
st.title("OLX Piasek - Panel sterowania")


def build_start_url_for_city(city_name: str) -> str:
    city_name = (city_name or "").strip()
    if not city_name:
        return f"{app.BASE_DOMAIN}{app.CATEGORY_PATH}q-{quote_plus('piasek')}/"

    # Google suggestion may return e.g. "Olkusz, Polska" -> keep first segment.
    city_core = city_name.split(",")[0].strip()
    normalized = unicodedata.normalize("NFKD", city_core).encode("ascii", "ignore").decode("ascii").lower()
    slug = normalized.replace(" ", "-")
    slug = "".join(ch for ch in slug if ch.isalnum() or ch == "-")
    slug = "-".join(part for part in slug.split("-") if part)
    return f"{app.BASE_DOMAIN}{app.CATEGORY_PATH}q-{quote_plus('piasek')}/{slug}/"


with st.sidebar:
    st.header("Ustawienia")
    city_input = st.text_input(
        "Miasto / miejscowość (edytowalne)",
        value="",
        placeholder="np. Kraków, Wieliczka, Pcim...",
    )
    whole_poland = st.checkbox("Cała Polska (bez lokalizacji)", value=False)
    custom_url = st.text_input(
        "Własny URL OLX (opcjonalnie)",
        value="",
        placeholder="Wklej pełny URL wyszukiwania OLX...",
        help="Jeśli podasz URL, ma on priorytet nad wyborem miasta.",
    )
    max_pages = st.number_input("Liczba stron", min_value=1, max_value=50, value=3, step=1)
    headless = st.checkbox("Praca w tle (headless)", value=True)
    incremental = st.checkbox("Tylko nowe oferty (incremental)", value=True)

    chosen_city = "" if whole_poland else city_input.strip()
    auto_url = build_start_url_for_city(chosen_city)
    effective_url = custom_url.strip() if custom_url.strip() else auto_url
    st.caption("URL generowany pod maską")
    st.code(effective_url, language=None)

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        start_clicked = st.button("Start", use_container_width=True)
    with col_b:
        stop_clicked = st.button("Stop", use_container_width=True)
    with col_c:
        refresh_clicked = st.button("Odśwież", use_container_width=True)

if start_clicked:
    app.START_URL = effective_url
    app.QUERY = "piasek"
    app.MAX_PAGES = int(max_pages)
    app.HEADLESS_DEFAULT = bool(headless)
    app.INCREMENTAL_ONLY_NEW = bool(incremental)
    app.start_scraping_background()
    st.success("Scraping uruchomiony.")

if stop_clicked:
    app.request_stop()
    st.warning("Wysłano żądanie zatrzymania. Scraper zakończy się bezpiecznie.")

if refresh_clicked:
    st.rerun()

st.subheader("Status")
st.json(app.SCRAPE_STATUS)

st.subheader("Zbiorczy plik ofert")
if app.OUTPUT_FILE.exists():
    df = pd.read_csv(app.OUTPUT_FILE, sep=";")
    st.dataframe(df, use_container_width=True, height=420)
else:
    st.info("Brak pliku z ofertami.")

st.subheader("Alerty (ostatnie 30 linii)")
if app.ALERTS_FILE.exists():
    lines = app.ALERTS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
    st.code("\n".join(lines[-30:]) if lines else "(pusto)")
else:
    st.info("Brak alertów.")
