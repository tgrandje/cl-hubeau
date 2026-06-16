#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 16:54:09 2024

Test mostly high level functions
"""

import geopandas as gpd
import pandas as pd
import pytest

from requests_cache import CacheMixin

from cl_hubeau import water_abstraction
from cl_hubeau.water_abstraction import AbstractionSession


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

        data = {
            "count": 1,
            "first": "blah_page",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "code_ouvrage": "dummy_code",
                    },
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                }
            ],
        }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


def test_get_one_ouvrage_live():
    with AbstractionSession() as session:
        data = session.get_ouvrages(nom_commune="Custines", format="geojson")
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) >= 3


def test_get_chronicles_live():
    df = water_abstraction.get_all_chronicles(code_departement="75")
    assert len(df) >= 60


def test_get_plvt_live():
    df = water_abstraction.get_all_points_prelevement(code_departement="59")
    assert len(df) >= 1000
