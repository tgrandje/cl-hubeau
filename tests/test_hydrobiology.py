#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 09:00:28 2025

Test mostly high level functions
"""

import geopandas as gpd
import pytest
from requests_cache import CacheMixin

from cl_hubeau import hydrobiology
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

        if "stations" in url in url:

            bbox = kwargs["params"].get("bbox")

            data = {
                "count": 1,
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "code_station_hydrobio": f"dummy_code_{bbox}",
                            "libelle_station_hydrobio": "dummy",
                        },
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    }
                ],
            }
        elif "taxons" in url or "indices" in url:

            data = {
                "count": 1,
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "code_station_hydrobio": "dummy_code",
                            "libelle_station_hydrobio": "dummy",
                            "date_prelevement": "2024-01-01",
                        },
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    },
                ],
            }

        return MockResponse(data)

    monkeypatch.setattr(CacheMixin, "request", mock_request)
    monkeypatch.setattr(cl_hubeau.utils.mesh, "_get_mesh", mock_get_mesh)


@silence_api_version_warning
def test_get_stations_mocked(mock_get_data):
    data = hydrobiology.get_all_stations(fill_values=False)
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 2


@silence_api_version_warning
def test_get_taxa_mocked(mock_get_data):
    data = hydrobiology.get_all_taxa(
        code_station_hydrobio="dummy_code",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-12-31",
    )
    data = data.drop_duplicates()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_indexes_conditions_mocked(mock_get_data):
    data = hydrobiology.get_all_indexes(
        code_station_hydrobio="dummy_code",
        date_debut_prelevement="2020-01-01",
        date_fin_prelevement="2020-12-31",
    )
    data = data.drop_duplicates()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


def test_get_stations_live():
    df = [
        hydrobiology.get_all_stations(fill_values=False),
        hydrobiology.get_all_stations(fill_values=False, code_region="32"),
        hydrobiology.get_all_stations(
            fill_values=False, code_station_hydrobio="08824101"
        ),
        hydrobiology.get_all_stations(
            fill_values=False, code_departement="59"
        ),
    ]
    assert all(isinstance(x, gpd.GeoDataFrame) for x in df)

    df = hydrobiology.get_all_stations(
        code_station_hydrobio="08824101", format="json"
    )
    assert isinstance(df, gpd.GeoDataFrame)
    assert len(df) == 1


def test_get_indexes_live():
    df = hydrobiology.get_all_indexes(
        code_departement="974",
        date_debut_prelevement="1995-01-01",
        date_fin_prelevement="2000-12-31",
    )
    assert isinstance(df, gpd.GeoDataFrame)
    assert len(df) == 80


def test_get_taxa_live():
    df = hydrobiology.get_all_taxa(
        code_departement="974",
        date_debut_prelevement="1995-01-01",
        date_fin_prelevement="2000-12-31",
    )
    assert isinstance(df, gpd.GeoDataFrame)
    assert len(df) == 169
