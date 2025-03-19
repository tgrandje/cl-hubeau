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

from cl_hubeau import water_services
from cl_hubeau.water_services import WaterServicesSession


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

        if "communes" in url:
            data = {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": {
                        "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                    }
                },
                "count": 1,
                "first": "some_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "nom_commune": "city_name",
                            "noms_service": [
                                "service_name"
                            ],
                            "indicateurs": {
                                "dummy_indicator": 0,
                            },
                            "code_commune_insee": "00000",
                            "annee": 2015,
                            "codes_service": [
                                000000
                            ]
                        },
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    }
                ]
            }
        elif "indicateurs" in url:
            data = {
                "count": 1,
                "first": "some_page",
                "next": None,
                "data": [
                    {
                        "code_service": 00000,
                        "nom_service": "service_name",
                        "codes_commune": [
                            "88010",
                        ],
                        "noms_commune": [
                            "Aouze",
                        ],
                        "numero_siren": "258800838",
                        "type_collectivite": "Syndicat Intercommunal à Vocation Unique",
                        "mode_gestion": "Délégation",
                        "annee": 2015,
                        "indicateur": 1,
                        "uri_indicateur": "some_uri"
                    }
                ]
            }

        elif "services" in url:
            data = {
                "count": 1,
                "first": "some_page",
                "data": [
                    {
                        "code_service": 0,
                        "nom_service": "service_name",
                        "codes_commune": [
                            "10000",
                        ],
                        "noms_commune": [
                            "city_name",
                        ],
                        "annee": 2015,
                        "indicateurs": {
                            "dummy_indicator": 0,
                        }
                    }
                ]
            }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


def test_get_indicators_mocked(mock_get_data):
    with WaterServicesSession() as session:
        data = session.get_indicators(code_indicateur="test_code")
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_all_services_mocked(mock_get_data):
    data = water_services.get_all_services()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_all_communes_mocked(mock_get_data):
    data = water_services.get_all_communes()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1
