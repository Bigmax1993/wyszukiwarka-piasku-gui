import logging

import pandas as pd

import olx_scraper_background as app


class DummyDriver:
    def quit(self):
        return None


def test_regression_price_parser_handles_decimal_comma():
    assert app.extract_price_to_number("99,50 zł") == 99.5


def test_regression_incremental_skips_urls_from_cache(monkeypatch, tmp_path):
    output_file = tmp_path / "final.csv"
    cache_file = tmp_path / "cache.json"
    status_file = tmp_path / "status.json"
    alerts_file = tmp_path / "alerts.log"

    cache_file.write_text(
        '{"seen_urls": ["https://www.olx.pl/d/oferta/1"], "visited_pages": [], "runs": 0}',
        encoding="utf-8",
    )

    monkeypatch.setattr(app, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(app, "OUTPUT_FILE", output_file)
    monkeypatch.setattr(app, "CACHE_FILE", cache_file)
    monkeypatch.setattr(app, "STATUS_FILE", status_file)
    monkeypatch.setattr(app, "ALERTS_FILE", alerts_file)
    monkeypatch.setattr(app, "MAX_PAGES", 1)
    monkeypatch.setattr(app, "INCREMENTAL_ONLY_NEW", True)
    monkeypatch.setattr(app, "setup_logging", lambda: logging.getLogger("regression_test"))
    monkeypatch.setattr(app, "build_driver", lambda headless=True: DummyDriver())

    monkeypatch.setattr(
        app,
        "scrape_page",
        lambda driver, page, logger: [
            {
                "tytul": "Stara oferta",
                "cena_pln": 120.0,
                "waluta": "PLN",
                "jednostka_ceny": "za_m3",
                "cena_znormalizowana_pln": 120.0,
                "cena_tekst": "120 zł / m3",
                "lokalizacja_data": "Krakow",
                "url": "https://www.olx.pl/d/oferta/1",
                "zrodlo": app.START_URL,
                "data_pobrania": "2026-01-01T10:00:00",
            }
        ],
    )

    app.run_scraper(headless_default=True)

    df = pd.read_csv(output_file, sep=";")
    assert len(df) == 0


def test_regression_request_stop_sets_flags(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(app, "STATUS_FILE", tmp_path / "status.json")
    monkeypatch.setattr(app, "ALERTS_FILE", tmp_path / "alerts.log")
    app.STOP_REQUESTED = False
    app.SCRAPE_STATUS["stop_requested"] = False

    app.request_stop()

    assert app.STOP_REQUESTED is True
    assert app.SCRAPE_STATUS["stop_requested"] is True
    assert (tmp_path / "status.json").exists()
    assert (tmp_path / "alerts.log").exists()
