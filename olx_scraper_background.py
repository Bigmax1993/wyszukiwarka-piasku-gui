import csv
import json
import logging
import random
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, quote_plus, unquote, urlencode, urljoin, urlsplit, urlunsplit

import pandas as pd
import psutil
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


# =========================
# KONFIG
# =========================
OUTPUT_DIR = Path(r"C:\Users\kanbu\Documents\Wyniki wyszuiwania piasku")
OUTPUT_FILE = OUTPUT_DIR / "olx_piasek_koncowy.csv"
CACHE_FILE = OUTPUT_DIR / "olx_cache.json"
STATUS_FILE = OUTPUT_DIR / "olx_status.json"
ALERTS_FILE = OUTPUT_DIR / "olx_alerts.log"
LOG_FILE = OUTPUT_DIR / "olx_scraper.log"

BASE_DOMAIN = "https://www.olx.pl"
QUERY = "piasek"
CATEGORY_PATH = "/budowa-i-remont/materialy-sypkie/"
START_URL = f"{BASE_DOMAIN}{CATEGORY_PATH}q-{quote_plus(QUERY)}/"
MAX_PAGES = 3
HEADLESS_DEFAULT = True
CAPTCHA_CHECK_TIMEOUT = 600
INCREMENTAL_ONLY_NEW = True
STOP_REQUESTED = False


class CaptchaRequired(Exception):
    pass


SCRAPE_STATUS = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "rows": 0,
    "new_rows": 0,
    "stop_requested": False,
    "selector_used": None,
    "output_csv": str(OUTPUT_FILE.resolve()),
    "cache_json": str(CACHE_FILE.resolve()),
    "status_json": str(STATUS_FILE.resolve()),
    "alerts_log": str(ALERTS_FILE.resolve()),
    "log_file": str(LOG_FILE.resolve()),
}


def persist_status():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.write_text(json.dumps(SCRAPE_STATUS, ensure_ascii=False, indent=2), encoding="utf-8")


def append_alert(level: str, message: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now().isoformat(timespec='seconds')} | {level} | {message}\n"
    with ALERTS_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


def setup_logging():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("olx_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def wait_for_user_confirmation(message):
    print(message)
    input("> ")


def request_stop():
    global STOP_REQUESTED
    STOP_REQUESTED = True
    SCRAPE_STATUS["stop_requested"] = True
    append_alert("INFO", "Otrzymano żądanie zatrzymania scrapingu.")
    persist_status()


def build_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
    else:
        options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.page_load_strategy = "eager"
    return webdriver.Chrome(options=options)


def search_url(page=1):
    if page <= 1:
        return START_URL
    parsed = urlsplit(START_URL)
    query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_items["page"] = str(page)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_items), parsed.fragment))


def save_csv(rows, path):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fields = [
        "tytul",
        "cena_pln",
        "waluta",
        "jednostka_ceny",
        "cena_znormalizowana_pln",
        "cena_tekst",
        "lokalizacja_data",
        "url",
        "zrodlo",
        "data_pobrania",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def load_existing_csv(path, logger):
    rows = []
    seen_urls = set()
    if not path.exists():
        return rows, seen_urls
    logger.info("Ładowanie istniejącego CSV: %s", path)
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            rows.append(row)
            if row.get("url"):
                seen_urls.add(row["url"])
    logger.info("Wczytano %s rekordów z CSV (seen=%s)", len(rows), len(seen_urls))
    return rows, seen_urls


def load_cache(logger):
    if not CACHE_FILE.exists():
        logger.info("Brak cache JSON - tworzę nowy.")
        return {"seen_urls": [], "visited_pages": [], "runs": 0}
    try:
        cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        cache.setdefault("seen_urls", [])
        cache.setdefault("visited_pages", [])
        cache.setdefault("runs", 0)
        logger.info("Wczytano cache JSON.")
        return cache
    except Exception as exc:
        logger.warning("Nie udało się wczytać cache JSON (%s) - tworzę nowy.", exc)
        return {"seen_urls": [], "visited_pages": [], "runs": 0}


def save_cache(cache, logger):
    try:
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Zapisano cache JSON.")
    except Exception as exc:
        logger.error("Błąd zapisu cache JSON: %s", exc)


def extract_price_to_number(price_text: str):
    if not price_text:
        return None
    clean = price_text.replace("\xa0", " ").replace("zł", "").strip().replace(" ", "").replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)", clean)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def extract_currency(price_text: str) -> str:
    txt = (price_text or "").lower()
    if "zł" in txt or "pln" in txt:
        return "PLN"
    if "eur" in txt or "€" in txt:
        return "EUR"
    if "usd" in txt or "$" in txt:
        return "USD"
    return "unknown"


def extract_price_unit(price_text: str, title_text: str = "") -> str:
    txt = f"{price_text} {title_text}".lower()
    checks = {
        "za_tone": [r"/\s*t", r"\bza\s*ton", r"\btona\b"],
        "za_m3": [r"/\s*m3", r"/\s*m\^?3", r"\bm3\b"],
        "za_kg": [r"/\s*kg", r"\bza\s*kg\b", r"\bkg\b"],
        "za_worek": [r"\bworek\b", r"\bworki\b"],
        "za_wywrotke": [r"\bwywrotk"],
    }
    for name, patterns in checks.items():
        if any(re.search(p, txt) for p in patterns):
            return name
    return "za_sztuke_lub_nieznana"


def normalize_price_pln(price_value, currency: str):
    if price_value is None:
        return None
    if currency == "PLN":
        return price_value
    return price_value


def dismiss_consent(driver):
    selectors = [
        (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR, "button[data-testid='cookie-policy-manage-dialog-accept-button']"),
        (By.CSS_SELECTOR, "button[data-testid='cookies-popup-accept']"),
    ]
    for by, selector in selectors:
        try:
            button = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, selector)))
            button.click()
            time.sleep(1)
            return True
        except Exception:
            continue
    return False


def wait_for_listing_presence(driver, timeout_seconds=20):
    selectors = [
        "a[data-cy='listing-ad-title']",
        "a[data-testid='ad-title']",
        "div[data-cy='l-card'] a[href*='/d/oferta/']",
    ]
    last_error = None
    for selector in selectors:
        try:
            WebDriverWait(driver, timeout_seconds).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            SCRAPE_STATUS["selector_used"] = selector
            return selector
        except TimeoutException as exc:
            last_error = exc
    raise last_error if last_error else TimeoutException("Brak listingu")


def is_captcha_page(driver):
    try:
        source = (driver.page_source or "").lower()
        url = (driver.current_url or "").lower()
    except Exception:
        return False
    patterns = ["captcha", "recaptcha", "unusual traffic", "/sorry/"]
    return any(p in source for p in patterns) or any(p in url for p in patterns)


def transfer_cookies(source_driver, target_driver):
    try:
        cookies = source_driver.get_cookies()
    except Exception:
        return
    target_driver.get(BASE_DOMAIN)
    for cookie in cookies:
        safe_cookie = {
            k: v
            for k, v in cookie.items()
            if k in {"name", "value", "domain", "path", "expiry", "secure", "httpOnly", "sameSite"}
        }
        try:
            target_driver.add_cookie(safe_cookie)
        except Exception:
            continue


def handle_captcha(driver, logger):
    logger.warning("Wykryto CAPTCHA/cookies-block. Otwieram widoczną przeglądarkę.")
    current_url = ""
    try:
        current_url = driver.current_url
    except Exception:
        pass

    visible_driver = None
    try:
        visible_driver = build_driver(headless=False)
        visible_driver.get(current_url or START_URL)
        dismiss_consent(visible_driver)
        wait_for_user_confirmation(
            "Rozwiąż cookies/captcha w oknie przeglądarki i naciśnij Enter tutaj, aby kontynuować."
        )

        start = time.time()
        while is_captcha_page(visible_driver):
            if STOP_REQUESTED:
                raise RuntimeError("Scraping zatrzymany przez użytkownika.")
            if time.time() - start > CAPTCHA_CHECK_TIMEOUT:
                raise TimeoutException("Przekroczono czas oczekiwania na captcha/cookies.")
            wait_for_user_confirmation("Nadal wykrywam blokadę. Dokończ i naciśnij Enter ponownie.")

        headless_driver = build_driver(headless=True)
        transfer_cookies(visible_driver, headless_driver)
        headless_driver.get(current_url or START_URL)
        logger.info("Powrót do działania w tle.")
        return headless_driver
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if visible_driver is not None:
            try:
                visible_driver.quit()
            except Exception:
                pass


def parse_listing_cards(driver, logger):
    cards = driver.find_elements(By.CSS_SELECTOR, "div[data-cy='l-card']")
    rows = []
    for card in cards:
        title_node = None
        for selector in [
            "a[data-cy='listing-ad-title']",
            "a[data-testid='ad-title']",
            "a[href*='/d/oferta/']",
        ]:
            found = card.find_elements(By.CSS_SELECTOR, selector)
            if found:
                title_node = found[0]
                break
        if not title_node:
            continue
        title = (title_node.text or "").strip()
        if not title:
            title = (title_node.get_attribute("textContent") or "").strip()
        if not title:
            title = (title_node.get_attribute("title") or "").strip()
        if not title:
            title = (title_node.get_attribute("aria-label") or "").strip()

        href = (title_node.get_attribute("href") or "").strip()
        if not href:
            continue
        url = urljoin(BASE_DOMAIN, href)
        if not title:
            # Fallback 1: build readable title from URL slug.
            title = extract_title_from_offer_url(url)
        if not title:
            # Fallback 2: open offer page and read title there.
            title = fetch_offer_title_from_detail_page(driver, url)
        if not title:
            title = "Brak tytułu"

        price_nodes = card.find_elements(By.CSS_SELECTOR, "p[data-testid='ad-price']")
        price_text = (price_nodes[0].text or "").strip() if price_nodes else ""
        price_pln = extract_price_to_number(price_text)
        currency = extract_currency(price_text)
        unit = extract_price_unit(price_text, title)
        normalized = normalize_price_pln(price_pln, currency)

        loc_nodes = card.find_elements(By.CSS_SELECTOR, "p[data-testid='location-date']")
        location_date = (loc_nodes[0].text or "").strip() if loc_nodes else ""

        rows.append(
            {
                "tytul": title,
                "cena_pln": price_pln,
                "waluta": currency,
                "jednostka_ceny": unit,
                "cena_znormalizowana_pln": normalized,
                "cena_tekst": price_text,
                "lokalizacja_data": location_date,
                "url": url,
                "zrodlo": START_URL,
                "data_pobrania": datetime.now().isoformat(timespec="seconds"),
            }
        )
    logger.info("Zparsowano %s kart na stronie.", len(rows))
    return rows


def extract_title_from_offer_url(offer_url: str) -> str:
    if not offer_url:
        return ""
    try:
        parsed = urlsplit(offer_url)
        path = unquote(parsed.path or "")
        # Typical path: /d/oferta/<slug>-CIDxxxx-IDyyyy.html
        last_segment = path.strip("/").split("/")[-1]
        if not last_segment:
            return ""

        no_ext = re.sub(r"\.html?$", "", last_segment, flags=re.IGNORECASE)
        # Remove OLX technical suffixes from the end.
        no_suffix = re.sub(r"(?:-CID[0-9A-Z]+)?(?:-ID[0-9A-Z]+)?$", "", no_ext, flags=re.IGNORECASE)
        cleaned = no_suffix.replace("-", " ").strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned
    except Exception:
        return ""


def fetch_offer_title_from_detail_page(driver, offer_url: str) -> str:
    title = ""
    try:
        response = requests.get(
            offer_url,
            timeout=15,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            },
        )
        if response.status_code != 200:
            return ""

        soup = BeautifulSoup(response.text, "lxml")
        h1_node = soup.select_one("h1")
        if h1_node:
            title = h1_node.get_text(" ", strip=True)
        if not title:
            meta_node = soup.select_one("meta[property='og:title']")
            if meta_node:
                title = (meta_node.get("content") or "").strip()
        if not title:
            title_node = soup.select_one("title")
            if title_node:
                title = title_node.get_text(" ", strip=True)
    except Exception:
        return ""
    return title


def scrape_page(driver, page_num, logger):
    url = search_url(page_num)
    logger.info("Otwieram stronę %s: %s", page_num, url)
    driver.get(url)
    time.sleep(random.uniform(1.2, 2.2))
    dismiss_consent(driver)

    if is_captcha_page(driver):
        raise CaptchaRequired("Captcha/cookie gate na wejściu strony.")

    try:
        wait_for_listing_presence(driver, timeout_seconds=20)
    except TimeoutException as exc:
        if is_captcha_page(driver):
            raise CaptchaRequired("Captcha/cookie gate zamiast listingu.") from exc
        raise

    return parse_listing_cards(driver, logger)


def run_scraper(headless_default=HEADLESS_DEFAULT):
    global STOP_REQUESTED
    STOP_REQUESTED = False
    logger = setup_logging()
    logger.info("=== START OLX scraper ===")

    SCRAPE_STATUS["running"] = True
    SCRAPE_STATUS["started_at"] = datetime.now().isoformat(timespec="seconds")
    SCRAPE_STATUS["finished_at"] = None
    SCRAPE_STATUS["error"] = None
    SCRAPE_STATUS["rows"] = 0
    SCRAPE_STATUS["new_rows"] = 0
    SCRAPE_STATUS["stop_requested"] = False
    persist_status()

    driver = build_driver(headless=headless_default)
    all_rows, seen_global = load_existing_csv(OUTPUT_FILE, logger)
    cache = load_cache(logger)

    try:
        seen_from_cache = set(cache.get("seen_urls", []))
        run_rows = []

        for page in range(1, MAX_PAGES + 1):
            if STOP_REQUESTED:
                logger.warning("Przerwano scraping na żądanie użytkownika.")
                append_alert("INFO", "Scraping zatrzymany przez użytkownika.")
                break
            retries = 0
            while True:
                if STOP_REQUESTED:
                    logger.warning("Przerwano scraping na żądanie użytkownika.")
                    append_alert("INFO", "Scraping zatrzymany przez użytkownika.")
                    break
                try:
                    page_rows = scrape_page(driver, page, logger)
                    cache["visited_pages"].append(search_url(page))
                    added = 0
                    for row in page_rows:
                        if STOP_REQUESTED:
                            break
                        url = row.get("url", "")
                        if not url:
                            continue
                        if url in seen_global:
                            continue
                        if INCREMENTAL_ONLY_NEW and url in seen_from_cache:
                            continue
                        seen_global.add(url)
                        run_rows.append(row)
                        all_rows.append(row)
                        added += 1
                    logger.info("Strona %s: +%s nowych ofert.", page, added)
                    save_csv(all_rows, OUTPUT_FILE)
                    save_cache(cache, logger)
                    break
                except CaptchaRequired as exc:
                    retries += 1
                    logger.warning("Strona %s: %s (próba %s)", page, exc, retries)
                    if retries > 3:
                        logger.error("Strona %s: zbyt wiele blokad, pomijam stronę.", page)
                        break
                    driver = handle_captcha(driver, logger)
                    time.sleep(1.5)
                except Exception as exc:
                    if STOP_REQUESTED:
                        break
                    logger.exception("Strona %s: błąd pobierania", page)
                    append_alert("ERROR", f"Błąd strony {page}: {exc}")
                    break
            if STOP_REQUESTED:
                break

        cache["seen_urls"] = sorted(seen_global)
        cache["runs"] = int(cache.get("runs", 0)) + 1
        cache["last_run_at"] = datetime.now().isoformat(timespec="seconds")
        cache["last_run_rows"] = len(all_rows)
        cache["last_run_new_rows"] = len(run_rows)
        save_cache(cache, logger)

        SCRAPE_STATUS["rows"] = len(all_rows)
        SCRAPE_STATUS["new_rows"] = len(run_rows)
        if run_rows:
            append_alert("SUCCESS", f"Zapisano {len(run_rows)} nowych ofert.")
        else:
            append_alert("INFO", "Brak nowych ofert w tym uruchomieniu.")
    except Exception as exc:
        SCRAPE_STATUS["error"] = str(exc)
        append_alert("ERROR", str(exc))
        logger.exception("Błąd główny scrapera")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        SCRAPE_STATUS["running"] = False
        SCRAPE_STATUS["finished_at"] = datetime.now().isoformat(timespec="seconds")
        persist_status()
        logger.info("=== KONIEC OLX scraper ===")


def start_scraping_background():
    if SCRAPE_STATUS["running"]:
        return None
    thread = threading.Thread(target=run_scraper, daemon=True, name="OLX-Scraper-Thread")
    thread.start()
    return thread


def show_scrape_status():
    print(json.dumps(SCRAPE_STATUS, ensure_ascii=False, indent=2))


def print_all_processes():
    print("\n=== WSZYSTKIE PROCESY ===")
    rows = []
    for proc in psutil.process_iter(["pid", "name", "status", "memory_info"]):
        try:
            mem_mb = round((proc.info["memory_info"].rss / (1024 * 1024)), 2) if proc.info["memory_info"] else 0
            rows.append((proc.info["pid"], proc.info["name"], proc.info["status"], mem_mb))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    for pid, name, status, mem in sorted(rows, key=lambda x: x[0]):
        print(f"{pid:>6} | {str(name):<35} | {str(status):<12} | {mem:>8} MB")
    print(f"\nŁącznie procesów: {len(rows)}")


def main():
    run_scraper(headless_default=HEADLESS_DEFAULT)


if __name__ == "__main__":
    main()
