#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Test high level functions
"""

from datetime import date, timedelta

import geopandas as gpd
import pandas as pd
import pytest
import re
from requests_cache import CacheMixin

from cl_hubeau import fish


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

        if re.search("stations$", url):
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
            re.search("operations$", url)
            or re.search("observations$", url)
            or re.search("indicateurs$", url)
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


def test_get_stations(mock_get_data):
    data = fish.get_all_stations()
    assert isinstance(data, gpd.GeoDataFrame)
    assert len(data) == 103


def test_get_observations(mock_get_data):
    data = fish.get_all_observations(
        code_station="dummy_code",
        date_operation_min="2020-01-01",
        date_operation_max="2020-05-31",
    )
    print(f"{data=}")
    data = data.drop_duplicates()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_indicators(mock_get_data):
    data = fish.get_all_indicators(
        code_station="dummy_code",
        date_operation_min="2020-01-01",
        date_operation_max="2020-12-31",
    )
    print(f"{data=}")
    data = data.drop_duplicates()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1
