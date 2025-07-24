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

from cl_hubeau import superficial_waterbodies_quality
from tests.utils import silence_api_version_warning


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

        if re.search("station_pc$", url):
            deps = kwargs["params"]["code_departement"].split(",")
            data = {
                "count": len(deps),
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "code_station": f"dummy_code_{dep}",
                            "libelle_station": "dummy_label",
                            "code_departement": dep,
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
                    for dep in deps
                ],
            }

        elif (
            re.search("operation_pc$", url)
            or re.search("condition_environnementale_pc$", url)
            or re.search("analyse_pc$", url)
        ):
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


@silence_api_version_warning
def test_get_stations_mocked(mock_get_data):
    data = superficial_waterbodies_quality.get_all_stations()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 103


@silence_api_version_warning
def test_get_operations_mocked(mock_get_data):
    data = superficial_waterbodies_quality.get_all_operations(
        code_station="dummy_code",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-12-31",
    )
    data = data.drop_duplicates()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_environmental_conditions_mocked(mock_get_data):
    data = superficial_waterbodies_quality.get_all_environmental_conditions(
        code_station="dummy_code",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-12-31",
    )
    data = data.drop_duplicates()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_analyses_mocked(mock_get_data):
    data = superficial_waterbodies_quality.get_all_analyses(
        code_station="dummy_code",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-12-31",
    )
    data = data.drop_duplicates()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_stations_live():
    data = superficial_waterbodies_quality.get_all_stations(code_region="06")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 29


def test_get_operations_live():
    data = superficial_waterbodies_quality.get_all_operations(
        code_region="06",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-06-01",
    )
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 29


def test_get_environmental_conditions_live():
    data = superficial_waterbodies_quality.get_all_environmental_conditions(
        code_departement="59",
        date_debut_prelevement="1990-01-01",
        date_fin_prelevement="1990-07-01",
    )
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 709


def test_get_analyses_live():
    data = superficial_waterbodies_quality.get_all_analyses(
        code_departement="974",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-06-01",
        libelle_parametre="Benzo(a)pyrène",
    )
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 800
