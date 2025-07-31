#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 09:00:28 2025

Test mostly high level functions
"""

import geopandas as gpd

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


# tests mockups à faire pour contrôler le bon nombre de résultats
