#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 16:14:50 2025

Test mostly high level functions
"""

import pandas as pd
import pytest

from requests_cache import CacheMixin

from cl_hubeau import phytopharmaceuticals_transactions
from cl_hubeau.phytopharmaceuticals_transactions import (
    PhytopharmaceuticalsSession,
)


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

        if "achats/substances" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "amm": "2140050",
                        "annee": 2022,
                        "achat_etranger": "Non",
                        "classification": "CMR",
                        "classification_mention": None,
                        "code_cas": "57966-95-7",
                        "code_substance": "1139",
                        "code_territoire": "FR",
                        "fonction": "Fongicide",
                        "libelle_substance": "cymoxanil",
                        "libelle_territoire": "France entière",
                        "quantite": 712.8716,
                        "type_territoire": "National",
                        "uri_substance": "http://id.eaufrance.fr/par/1139",
                        "uri_territoire": "-",
                    },
                ],
            }
        elif "achats/produits" in url:

            # Data with duplicates to check that duplicates are removed!
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "achat_etranger": "Non",
                        "annee": 2022,
                        "amm": "2160996",
                        "code_territoire": "FR",
                        "eaj": "Non",
                        "libelle_territoire": "France entière",
                        "type_territoire": "National",
                        "uri_territoire": "-",
                        "quantite": 12400,
                        "unite": "l",
                    }
                ],
            }

        elif "ventes/substances" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "amm": "2150353",
                        "annee": 2022,
                        "classification": "Env A",
                        "classification_mention": "Substitution",
                        "code_cas": "1317-39-1",
                        "code_substance": "5563",
                        "code_territoire": "FR",
                        "fonction": "Fongicide",
                        "libelle_substance": "cuivre de l'oxyde cuivreux",
                        "quantite": 5467.5,
                        "libelle_territoire": "France entière",
                        "type_territoire": "National",
                        "uri_substance": "http://id.eaufrance.fr/par/5563",
                        "uri_territoire": "-",
                    }
                ],
            }

        elif "ventes/substances" in url:
            data = {
                "count": 1,
                "first": "blah_page",
                "data": [
                    {
                        "annee": 2022,
                        "amm": "2160839",
                        "eaj": "Non",
                        "code_territoire": "FR",
                        "libelle_territoire": "France entière",
                        "type_territoire": "National",
                        "uri_territoire": "-",
                        "quantite": 17208.8,
                        "unite": "l",
                    },
                ],
            }

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


def test_sold_substances_mocked(mock_get_data):
    df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        amm="2140050",
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_sold_products_mocked(mock_get_data):
    # TODO
    pass


def test_bought_substances_mocked(mock_get_data):
    # TODO
    pass


def test_bought_products_mocked(mock_get_data):
    # TODO
    pass


def test_sold_substances_live():
    df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        amm="2140050",
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_sold_products_live():
    # TODO
    pass


def test_bought_substances_live():
    # TODO
    pass


def test_bought_products_live():
    # TODO
    pass
