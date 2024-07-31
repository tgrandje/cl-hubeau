#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 15:49:09 2024

Test mostly high level functions
"""

import geopandas as gpd
import pandas as pd
import pytest
from requests_cache import CacheMixin

from cl_hubeau import piezometry
from cl_hubeau.piezometry import PiezometrySession


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
        elif "chroniques_tr" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "code_bss": "dummy_code",
                        "urn_bss": "dummy",
                        "timestamp_mesure": 1704063600000,
                        "niveau_eau_ngf": 0,
                        "longitude": 0,
                        "latitude": 0,
                        "bss_id": "dummy",
                        "altitude_station": 0,
                        "altitude_repere": 0,
                        "date_mesure": "2024-01-01T00:00:00Z",
                        "profondeur_nappe": 0,
                        "date_maj": "2024-01-01",
                    }
                ],
            }

        elif "chroniques" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "code_bss": "dummy_code",
                        "urn_bss": "dummy",
                        "date_mesure": "2024-01-01",
                        "timestamp_mesure": 1704063600000,
                        "niveau_nappe_eau": 0,
                        "mode_obtention": "dummy",
                        "statut": "dummy",
                        "qualification": "dummy",
                        "code_continuite": "dummy",
                        "nom_continuite": "dummy",
                        "code_producteur": "dummy",
                        "nom_producteur": "dummy",
                        "code_nature_mesure": None,
                        "nom_nature_mesure": None,
                        "profondeur_nappe": 0,
                    }
                ],
            }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


def test_get_all_stations_mocked(mock_get_data):
    data = piezometry.get_all_stations()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


def test_get_chronicles_mocked(mock_get_data):
    data = piezometry.get_chronicles(codes_bss=["dummy_code"])
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_chronicles_real_time_mocked(mock_get_data):
    data = piezometry.get_realtime_chronicles(codes_bss=["dummy_code"])
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_one_station_live():
    with PiezometrySession() as session:
        data = session.get_stations(
            code_bss=["07548X0009/F"], format="geojson"
        )
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


def test_get_chronicles_live():
    data = piezometry.get_chronicles(
        codes_bss=["07548X0009/F"],
        fields=["timestamp_mesure", "niveau_nappe_eau", "date_mesure"],
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 5000
    assert data.shape[1] == 3


def test_get_chronicles_real_time_live():
    data = piezometry.get_realtime_chronicles(
        codes_bss=["07548X0009/F"],
        fields=["timestamp_mesure", "niveau_eau_ngf", "date_mesure"],
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 1000
