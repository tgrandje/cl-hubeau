#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  14 10:31:32 2024

Test mostly high level functions
"""

import pandas as pd
import pytest
from requests_cache import CacheMixin

from cl_hubeau import ground_water_quality
from .utils import silence_api_version_warning


class MockResponse:
    def __init__(self, json_data):
        self.json_data = json_data
        self.ok = True

    def json(self):
        return self.json_data


@pytest.fixture
def mock_get_data(monkeypatch):

    def mock_request(*args, **kwargs):
        self, method, url, *args = args

        if "stations" in url:
            data = {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"},
                },
                "count": 1,
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "code_bss": "dummy_code",
                            "bss_id": "dummy",
                        },
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    }
                ],
            }
        elif "analyses" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "code_bss": "dummy_code",
                        "timestamp_mesure": 1704063600000,
                        "bss_id": "dummy",
                    }
                ],
            }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


@silence_api_version_warning
def test_get_all_stations_mocked(mock_get_data):
    data = ground_water_quality.get_all_stations()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_analyses_mocked(mock_get_data):
    data = ground_water_quality.get_all_analyses(
        bss_id="dummy_code",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-12-31",
    )
    # remove duplicates issued from the mockup
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 2  # 2 x 6 months


def test_get_one_station_live():
    with ground_water_quality.GroundWaterQualitySession() as session:
        data = session.get_stations(
            bss_id="01832B0600",
            fields=["bss_id"],
        )
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (1, 1)


def test_get_analyses_live():
    data = ground_water_quality.get_all_analyses(
        bss_id=["BSS000BMMA"],
        code_insee_actuel="59350",
        code_param="1461",
        fields=[
            "code_param",
            "resultat",
            "date_debut_prelevement",
        ],
    )
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (1, 3)
