#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 15:49:09 2024

Test only high level functions
"""

import geopandas as gpd
import pandas as pd
import pytest
from requests_cache import CacheMixin

from cl_hubeau import piezometry


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
                            "code_departement": "01",
                            "urns_masse_eau_edl": ["dummy"],
                            "date_maj": "Mon Jan 1 00:00:38 CEST 2024",
                            "nb_mesures_piezo": 1,
                            "codes_masse_eau_edl": ["dummy"],
                            "profondeur_investigation": 0,
                            "urn_bss": "dummy",
                            "nom_departement": "dummy",
                            "code_commune_insee": "00000",
                            "urns_bdlisa": ["dummy"],
                            "libelle_pe": "dummy",
                            "noms_masse_eau_edl": ["dummy"],
                            "date_fin_mesure": "2024-01-01",
                            "date_debut_mesure": "2024-01-01",
                            "codes_bdlisa": ["dummy"],
                            "altitude_station": "0",
                            "code_bss": "dummy_code",
                            "nom_commune": "dummy",
                            "x": 0,
                            "y": 0,
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
