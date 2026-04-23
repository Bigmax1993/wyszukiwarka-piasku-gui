import logging

import pandas as pd

import olx_scraper_background as app


class DummyDriver:
    def quit(self):
        return None


def test_run_scraper_writes_outputs_with_mocked_scrape(monkeypatch, tmp_path):
    output_file = tmp_path / "final.csv"
    cache_file = tmp_path / "cache.json"
    status_file = tmp_path / "status.json"
    alerts_file = tmp_path / "alerts.log"

    monkeypatch.setattr(app, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(app, "OUTPUT_FILE", output_file)
    monkeypatch.setattr(app, "CACHE_FILE", cache_file)
    monkeypatch.setattr(app, "STATUS_FILE", status_file)
    monkeypatch.setattr(app, "ALERTS_FILE", alerts_file)
    monkeypatch.setattr(app, "MAX_PAGES", 2)
    monkeypatch.setattr(app, "INCREMENTAL_ONLY_NEW", True)
    monkeypatch.setattr(app, "setup_logging", lambda: logging.getLogger("integration_test"))
    monkeypatch.setattr(app, "build_driver", lambda headless=True: DummyDriver())

    page_payload = {
        1: [
            {
                "tytul": "Piasek 100",
                "cena_pln": 100.0,
                "waluta": "PLN",
                "jednostka_ceny": "za_m3",
                "cena_znormalizowana_pln": 100.0,
                "cena_tekst": "100 zł / m3",
                "lokalizacja_data": "Krakow",
                "url": "https://www.olx.pl/d/oferta/1",
                "zrodlo": app.START_URL,
                "data_pobrania": "2026-01-01T10:00:00",
            }
        ],
        2: [
            {
                "tytul": "Piasek 200",
                "cena_pln": 200.0,
                "waluta": "PLN",
                "jednostka_ceny": "za_tone",
                "cena_znormalizowana_pln": 200.0,
                "cena_tekst": "200 zł / t",
                "lokalizacja_data": "Warszawa",
                "url": "https://www.olx.pl/d/oferta/2",
                "zrodlo": app.START_URL,
                "data_pobrania": "2026-01-01T10:05:00",
            }
        ],
    }

    monkeypatch.setattr(app, "scrape_page", lambda driver, page, logger: page_payload[page])

    app.run_scraper(headless_default=True)

    assert output_file.exists()
    assert cache_file.exists()
    assert status_file.exists()
    assert alerts_file.exists()
    assert app.SCRAPE_STATUS["error"] is None
    assert app.SCRAPE_STATUS["new_rows"] == 2
    assert app.SCRAPE_STATUS["stop_requested"] is False

    df = pd.read_csv(output_file, sep=";")
    assert len(df) == 2
    assert set(df["url"].tolist()) == {
        "https://www.olx.pl/d/oferta/1",
        "https://www.olx.pl/d/oferta/2",
    }
