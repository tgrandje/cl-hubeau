#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 09:00:28 2025

Test mostly high level functions
"""

import geopandas as gpd
import pandas as pd
import pytest

from requests_cache import CacheMixin

from cl_hubeau import hydrobiology


# class MockResponse:
#     def __init__(self, json_data):
#         self.json_data = json_data
#         self.ok = True

#     def json(self):
#         return self.json_data


# @pytest.fixture
# def mock_get_data(monkeypatch):

#     def mock_request(*args, **kwargs):
#         self, method, url, *args = args

#         if "stations" in url or "observations" in url:
#             data = {
#                 "count": 1,
#                 "first": "blah_page",
#                 "features": [
#                     {
#                         "type": "Feature",
#                         "properties": {
#                             "code_station": "dummy_code",
#                             "libelle_station": "dummy",
#                         },
#                         "geometry": {"type": "Point", "coordinates": [0, 0]},
#                     }
#                 ],
#             }
#         elif "observations" in url:

#             # Data with duplicates to check that duplicates are removed!
#             data = {
#                 "count": 1,
#                 "first": "blah_page",
#                 "features": [
#                     {
#                         "type": "Feature",
#                         "properties": {
#                             "code_station": "dummy_code",
#                             "libelle_station": "dummy",
#                             "date_observation": "2024-01-01",
#                         },
#                         "geometry": {"type": "Point", "coordinates": [0, 0]},
#                     },
#                     {
#                         "type": "Feature",
#                         "properties": {
#                             "code_station": "dummy_code",
#                             "libelle_station": "dummy",
#                             "date_observation": "2024-01-01",
#                         },
#                         "geometry": {"type": "Point", "coordinates": [0, 0]},
#                     },
#                 ],
#             }

#         elif "campagnes" in url:
#             data = {
#                 "count": 1,
#                 "first": "blah_campagne",
#                 "next": None,
#                 "data": [
#                     {
#                         "code_campagne": "dummy",
#                         "date_campagne": "2011-10-20",
#                     }
#                 ],
#             }

#         return MockResponse(data)

#     # init = CachedSession.request
#     monkeypatch.setattr(CacheMixin, "request", mock_request)


def test_get_stations_live():
    df = [
        hydrobiology.get_all_stations(),
        hydrobiology.get_all_stations(code_region="32"),
        hydrobiology.get_all_stations(code_station_hydrobio="08824101"),
        hydrobiology.get_all_stations(code_departement="59"),
    ]
    assert all(isinstance(x, gpd.GeoDataFrame) for x in df)

    df = (
        hydrobiology.get_all_stations(
            code_station_hydrobio="08824101", format="json"
        ),
    )
    assert not isinstance(df, gpd.GeoDataFrame)
    assert len(df) == 1


# def test_get_one_campaign_live():
#     with WatercoursesFlowSession() as session:
#         data = session.get_campaigns(code_campagne=[12])
#     assert isinstance(data, pd.DataFrame)
#     assert len(data) == 1


# def test_get_all_stations_mocked(mock_get_data):
#     data = watercourses_flow.get_all_stations()
#     assert isinstance(data, gpd.GeoDataFrame)
#     assert len(data) == 1


# def test_get_all_observations_mocked(mock_get_data):
#     data = watercourses_flow.get_all_observations()
#     assert isinstance(data, gpd.GeoDataFrame)
#     assert len(data) == 1


# def get_get_all_campaigns_live():
#     df = watercourses_flow.get_all_campaigns()
#     assert isinstance(df, pd.DataFrame)
#     assert len(df) >= 8000


if __name__ == "__main__":
    test_get_stations_live()
