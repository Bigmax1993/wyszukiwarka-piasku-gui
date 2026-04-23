# Wyszukiwarka piasku (OLX)

Projekt składa się z dwóch warstw:

- `olx_scraper_background.py` - silnik scrapingu OLX (Selenium, cache, retry, captcha/cookies recovery, zapis plików),
- `streamlit_app.py` - UI do sterowania scraperem (wybór miasta/miejscowości, start/stop, podgląd statusu i wyników).

## Lokalizacje

- Kod: `C:\Users\kanbu\Documents\Wyszukiwarka piasku`
- Wyniki: `C:\Users\kanbu\Documents\Wyniki wyszuiwania piasku`

## Co robi scraper

- pobiera ogłoszenia OLX dla zapytania o piasek,
- przechodzi po kolejnych stronach (`MAX_PAGES`),
- parsuje dane ofert (tytuł, cena, lokalizacja, URL itd.),
- rozpoznaje walutę i jednostkę ceny,
- zapisuje dane do CSV i cache JSON,
- działa incrementalnie (tylko nowe URL-e),
- obsługuje blokady cookies/captcha (przełączenie na widoczną przeglądarkę),
- umożliwia bezpieczne zatrzymanie (`request_stop`).

## Pliki wynikowe

- `olx_piasek_koncowy.csv` - jedyny zbiorczy plik ofert (delimiter `;`),
- `olx_cache.json` - cache (m.in. `seen_urls`, `visited_pages`, `runs`),
- `olx_status.json` - aktualny status uruchomienia,
- `olx_alerts.log` - alerty `SUCCESS/INFO/ERROR`,
- `olx_scraper.log` - log wykonania.

## Wymagania

- Python 3.10+
- Chrome
- biblioteki z `requirements.txt`:
  - `pandas`
  - `selenium`
  - `beautifulsoup4`
  - `lxml`
  - `psutil`
  - `pytest`
  - `streamlit`

## Instalacja

```powershell
cd "C:\Users\kanbu\Documents\Wyszukiwarka piasku"
pip install -r requirements.txt
```

## Uruchomienie

### CLI / skrypt

```powershell
python olx_scraper_background.py
```

### UI Streamlit

```powershell
streamlit run "C:\Users\kanbu\Documents\Wyszukiwarka piasku\streamlit_app.py"
```

## UI Streamlit - funkcje

- edytowalne pole `Miasto / miejscowość` (możesz wpisać dowolną nazwę ręcznie),
- opcjonalne pole `Własny URL OLX` (pełny link z najwyższym priorytetem),
- opcja `Cała Polska`,
- automatyczne budowanie URL pod maską,
- parametry `Liczba stron`, `headless`, `incremental`,
- przycisk `Start`,
- przycisk `Stop` (bezpieczne zatrzymanie),
- podgląd statusu, zbiorczego CSV i alertów.

## Główne parametry w `olx_scraper_background.py`

- `MAX_PAGES`
- `HEADLESS_DEFAULT`
- `CAPTCHA_CHECK_TIMEOUT`
- `INCREMENTAL_ONLY_NEW`
- `OUTPUT_DIR`
- `START_URL` / `QUERY`

## Testy

Testy są w katalogu `tests`:

- `test_unit_olx_scraper.py`
- `test_integration_olx_scraper.py`
- `test_regression_olx_scraper.py`

Uruchomienie wszystkich testów:

```powershell
cd "C:\Users\kanbu\Documents\Wyszukiwarka piasku"
python -m pytest -q
```
