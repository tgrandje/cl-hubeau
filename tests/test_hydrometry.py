#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 15:49:09 2024

Test mostly high level functions
"""

from datetime import date, timedelta

import geopandas as gpd
import pandas as pd
import pytest
from requests_cache import CacheMixin

from cl_hubeau import hydrometry
from cl_hubeau.hydrometry import HydrometrySession
from tests.utils import silence_api_version_warning

# "get_all_stations",
# "get_observations",
# "get_realtime_observations",


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
                            "code_station": "dummy",
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
        elif "sites" in url:
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
                            "code_site": "dummy",
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
        elif "observations_tr" in url:
            data = {
                "count": 1,
                "first": "blah_cursor",
                "next": None,
                "data": [
                    {
                        "code_site": "dummy",
                        "code_station": "dummy",
                        "grandeur_hydro": "H",
                        "date_obs": "2024-01-01T00:00:00Z",
                        "resultat_obs": 788,
                    }
                ],
            }
        elif "obs_elab" in url:
            data = {
                "count": 1,
                "first": "blah_cursor",
                "next": None,
                "data": [
                    {
                        "code_site": "dummy_code",
                        "code_station": "dummy_code",
                        "date_obs_elab": "2014-01-01",
                        "resultat_obs_elab": 58,
                        "grandeur_hydro_elab": "QmJ",
                    }
                ],
            }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


@silence_api_version_warning
def test_get_all_stations_mocked(mock_get_data):
    data = hydrometry.get_all_stations()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_all_sites_mocked(mock_get_data):
    data = hydrometry.get_all_sites()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_chronicles_mocked(mock_get_data):
    data = hydrometry.get_observations(code_entite=["dummy_code"])
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_chronicles_real_time_mocked(mock_get_data):
    data = hydrometry.get_realtime_observations(code_entite=["dummy_code"])
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_all_stations_live():
    data = hydrometry.get_all_stations(code_region="06")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 21
    assert data.code_region.apply(lambda x: "06" in x).all()

    data = hydrometry.get_all_stations(code_departement="93")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 1
    assert data.code_departement.apply(lambda x: "93" in x).all()

    data = hydrometry.get_all_stations(code_commune_station="75056")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 7
    assert data.code_commune_station.apply(lambda x: "75056" in x).all()


def test_get_all_sites_live():
    data = hydrometry.get_all_sites(code_region="06")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 21
    assert data.code_region.apply(lambda x: "06" in x).all()

    data = hydrometry.get_all_sites(code_departement="93")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 2
    assert data.code_departement.apply(lambda x: "93" in x).all()

    data = hydrometry.get_all_sites(code_commune_site="75056")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 1
    assert data.code_commune_site.apply(lambda x: "75056" in x).all()


def test_get_one_station_live():
    with HydrometrySession() as session:
        data = session.get_stations(
            code_station=["K437311001"], format="geojson"
        )
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


def test_get_one_sites_live():
    with HydrometrySession() as session:
        data = session.get_stations(code_site=["K4373110"], format="geojson")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


def test_get_chronicles_live():
    data = hydrometry.get_observations(
        code_entite=["K437311001"],
        fields=["resultat_obs_elab", "date_obs_elab"],
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 3000
    assert data.shape[1] == 2

    data = hydrometry.get_observations(
        code_region="06",
        fields=["resultat_obs_elab", "date_obs_elab", "code_region"],
        date_debut_obs_elab="2020-01-01",
        date_fin_obs_elab="2020-02-01",
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 3255

    data = hydrometry.get_observations(
        code_departement="93",
        fields=["resultat_obs_elab", "date_obs_elab"],
        date_debut_obs_elab="2020-01-01",
        date_fin_obs_elab="2020-02-01",
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 374

    data = hydrometry.get_observations(
        code_commune="90089",
        fields=["resultat_obs_elab", "date_obs_elab"],
        date_debut_obs_elab="2020-01-01",
        date_fin_obs_elab="2020-02-01",
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 646


def test_get_chronicles_real_time_live():
    data = hydrometry.get_realtime_observations(
        code_entite=["K437311001"],
        fields=["grandeur_hydro", "resultat_obs", "date_obs"],
        timestep=60,
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 100

    today = date.today().strftime("%Y-%m-%d")
    past = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    data = hydrometry.get_realtime_observations(
        code_region="03",
        fields=["resultat_obs_elab", "date_obs_elab", "code_region"],
        date_debut_obs=past,
        date_fin_obs=today,
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 2000

    data = hydrometry.get_realtime_observations(
        code_departement="93",
        fields=["resultat_obs_elab", "date_obs_elab", "code_region"],
        date_debut_obs=past,
        date_fin_obs=today,
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 300

    data = hydrometry.get_realtime_observations(
        code_commune="90089",
        fields=["resultat_obs_elab", "date_obs_elab", "code_region"],
        date_debut_obs=past,
        date_fin_obs=today,
    )
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 100
