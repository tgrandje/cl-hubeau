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

from cl_hubeau import watercourses_flow
from cl_hubeau.watercourses_flow import WatercoursesFlowSession


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

        if "stations" in url or "observations" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "code_station": "dummy_code",
                            "libelle_station": "dummy",
                        },
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    }
                ],
            }
        elif "observations" in url:

            # Data with duplicates to check that duplicates are removed!
            data = {
                "count": 1,
                "first": "blah_page",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "code_station": "dummy_code",
                            "libelle_station": "dummy",
                            "date_observation": "2024-01-01",
                        },
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    },
                    {
                        "type": "Feature",
                        "properties": {
                            "code_station": "dummy_code",
                            "libelle_station": "dummy",
                            "date_observation": "2024-01-01",
                        },
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    },
                ],
            }

        elif "campagnes" in url:
            data = {
                "count": 1,
                "first": "blah_campagne",
                "next": None,
                "data": [
                    {
                        "code_campagne": "dummy",
                        "date_campagne": "2011-10-20",
                    }
                ],
            }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


def test_get_one_station_live():
    with WatercoursesFlowSession() as session:
        data = session.get_stations(
            code_station=["D0110001"], format="geojson"
        )
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


def test_get_one_campagne_live():
    with WatercoursesFlowSession() as session:
        data = session.get_campagnes(code_campagne=[12])
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_all_stations_mocked(mock_get_data):
    data = watercourses_flow.get_all_stations()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1


def test_get_all_observations_mocked(mock_get_data):
    data = watercourses_flow.get_all_observations()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 1
