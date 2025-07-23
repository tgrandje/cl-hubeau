#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 16:31:32 2024

Test mostly high level functions
"""

import pandas as pd
import pytest
from requests_cache import CacheMixin

from cl_hubeau import drinking_water_quality
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

        if "communes_udi" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "code_commune": "00000",
                        "nom_commune": "DUMMY",
                        "nom_quartier": "DUMMY",
                        "code_reseau": "000000000",
                        "nom_reseau": "DUMMY",
                        "debut_alim": "2024-01-01",
                        "annee": "2024",
                    },
                ],
            }
        elif "resultats_dis" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "resultat_numerique": 0,
                        "date_prelevement": "2024-01-01T00:00:00Z",
                        "reseaux": [{"code": "000000000", "nom": "DUMMY"}],
                    }
                ],
            }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


@silence_api_version_warning
def test_get_all_networks_mocked(mock_get_data):
    data = drinking_water_quality.get_all_water_networks()
    # remove duplicates issued from the mockup
    data = data.drop_duplicates()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


@silence_api_version_warning
def test_get_control_results_mocked(mock_get_data):
    data = drinking_water_quality.get_control_results(code_reseau="dummy")
    # remove duplicates issued from the mockup
    data = data.drop_duplicates("date_prelevement")
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_one_station_live():
    with drinking_water_quality.DrinkingWaterQualitySession() as session:
        data = session.get_cities_networks(
            code_commune=["59350"],
            annee="2023",
            fields=["code_commune", "code_reseau", "debut_alim"],
        )
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (3, 3)


def test_get_control_results_live():
    data = drinking_water_quality.get_control_results(
        code_commune="59350",
        code_parametre="1340",
        date_min_prelevement="2023-01-01",
        date_max_prelevement="2023-12-31",
        fields=[
            "code_parametre",
            "resultat_numerique",
            "date_prelevement",
        ],
    )
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (11, 3)
