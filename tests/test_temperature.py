#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Test high level functions
"""

import geopandas as gpd
import pandas as pd
import pytest
import re
from requests_cache import CacheMixin

from cl_hubeau import temperature
import cl_hubeau.utils.mesh
from tests.utils import silence_api_version_warning


class MockResponse:
    def __init__(self, json_data):
        self.json_data = json_data
        self.ok = True

    def json(self):
        return self.json_data


@pytest.fixture
def mock_get_data(monkeypatch):

    def mock_get_mesh(*args, **kwargs):
        return [[0, 0, 1, 1], [1, 1, 2, 2]]

    def mock_request(*args, **kwargs):
        self, method, url, *args = args

        if re.search("station$", url):
            data = {
                "count": 1,
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "code_station": f"dummy_code_{kwargs}",
                            "libelle_station": "dummy_label",
                        },
                        "geometry": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                                },
                            },
                            "coordinates": [0, 0],
                        },
                    }
                ],
            }

        elif re.search("chronique$", url):
            code = kwargs["params"]["code_station"]
            data = {
                "count": 1,
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "date": "2020-06-01",
                            "code_station": code,
                        },
                        "geometry": {
                            "type": "Point",
                            "crs": {
                                "type": "name",
                                "properties": {
                                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                                },
                            },
                            "coordinates": [0, 0],
                        },
                    }
                ],
            }

        return MockResponse(data)

    monkeypatch.setattr(CacheMixin, "request", mock_request)
    monkeypatch.setattr(cl_hubeau.utils.mesh, "_get_mesh", mock_get_mesh)


@silence_api_version_warning
def test_get_stations_mocked(mock_get_data):
    data = temperature.get_all_stations(fill_values=False)
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 2


@silence_api_version_warning
def test_get_chronicles_mocked(mock_get_data):
    data = temperature.get_all_chronicles(
        code_station="dummy_code",
        date_debut_mesure="2020-01-01",
        date_fin_mesure="2020-12-31",
    )
    data = data.drop_duplicates()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_stations_live():
    data = temperature.get_all_stations(code_region="04")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 20

    data = temperature.get_all_stations(code_departement="90")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 1

    data = temperature.get_all_stations(code_commune="90048")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 1

    data = temperature.get_all_stations(code_bassin="A")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 15

    data = temperature.get_all_stations(code_sous_bassin="FRA_ESCA")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 15


def test_get_chronicles_live():
    data = temperature.get_all_chronicles(
        code_region="04",
        date_debut_mesure="2020-01-01",
        date_fin_mesure="2020-06-01",
        fields=[
            "code_station",
            "resultat",
            "date_mesure_temp",
            "heure_mesure_temp",
        ],
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) >= 165_000
