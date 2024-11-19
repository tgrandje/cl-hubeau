#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  14 10:31:32 2024

Test mostly high level functions
"""

import pandas as pd
import pytest
from requests_cache import CacheMixin

from cl_hubeau import ground_water_quality


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


def test_get_all_stations_mocked(mock_get_data):
    data = ground_water_quality.get_all_stations()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_analyses_mocked(mock_get_data):
    data = ground_water_quality.get_analyses(codes_entites=["dummy_code"])
    # remove duplicates issued from the mockup
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 1


def test_get_one_station_live():
    with ground_water_quality.GroundWaterQualitySession() as session:
        data = session.get_stations(
            code_commune=["59350"],
            annee="2023",
            fields=["code_commune", "code_bss"],
        )
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (3, 3)


def test_get_analyses_live():
    data = ground_water_quality.get_analyses(
        codes_communes="59350",
        code_param="1340",
        date_debut_prelevement="2023-01-01",
        date_fin_prelevement="2023-12-31",
        fields=[
            "code_parametre",
            "resultat_numerique",
            "date_prelevement",
        ],
    )
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (11, 3)
