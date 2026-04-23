import logging

import olx_scraper_background as app


def test_extract_price_to_number_parses_common_price():
    assert app.extract_price_to_number("1 299 zł") == 1299.0


def test_extract_price_to_number_returns_none_for_invalid_price():
    assert app.extract_price_to_number("do uzgodnienia") is None


def test_extract_currency_detects_pln():
    assert app.extract_currency("250 zł") == "PLN"


def test_extract_price_unit_detects_m3():
    assert app.extract_price_unit("120 zł / m3", "Piasek płukany") == "za_m3"


def test_load_cache_returns_default_structure_when_missing(tmp_path):
    logger = logging.getLogger("test_unit")
    app.CACHE_FILE = tmp_path / "missing_cache.json"
    cache = app.load_cache(logger)
    assert cache["runs"] == 0
    assert cache["visited_pages"] == []
    assert cache["seen_urls"] == []


def test_search_url_builds_paginated_urls(monkeypatch):
    monkeypatch.setattr(app, "START_URL", "https://example.com/base")
    assert app.search_url(1) == "https://example.com/base"
    assert app.search_url(3) == "https://example.com/base?page=3"
