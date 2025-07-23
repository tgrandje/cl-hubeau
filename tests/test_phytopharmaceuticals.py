#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 16:14:50 2025

Test mostly high level functions
"""

from datetime import date

import pandas as pd
import pytest

from requests_cache import CacheMixin

from cl_hubeau import phytopharmaceuticals_transactions
from cl_hubeau.phytopharmaceuticals_transactions import (
    PhytopharmaceuticalsSession,
)
from cl_hubeau.phytopharmaceuticals_transactions.utils import (
    _get_territory_years_combination,
)
from .utils import silence_api_version_warning


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
            data = {
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
        elif "ventes/produits" in url:
            data = {
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

        data.update({"count": 1, "first": "blah_page", "api_version": "v1"})

        return MockResponse(data)

    # init = CachedSession.request
    monkeypatch.setattr(CacheMixin, "request", mock_request)


@silence_api_version_warning
def test_sold_substances_mocked(mock_get_data):
    df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        code_substance="1139",
        amm="2140050",
        libelle_substance="cymoxanil",
        fonction="fongicide",
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


@silence_api_version_warning
def test_sold_products_mocked(mock_get_data):
    df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        amm="2140050",
        unite=["l", "kg"],
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


@silence_api_version_warning
def test_bought_substances_mocked(mock_get_data):
    df = phytopharmaceuticals_transactions.get_all_active_substances_bought(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        code_substance="5563",
        amm="2150353",
        achat_etranger="Oui",
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


@silence_api_version_warning
def test_bought_products_mocked(mock_get_data):
    df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        amm="2160839",
        achat_etranger="Non",
        unite=["l", "kg"],
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_sold_substances_live():
    df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        code_substance="1139",
        amm="2140050",
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_sold_products_live():
    df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_sold(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        amm="2140050",
        unite=["l", "kg"],
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_bought_substances_live():
    df = phytopharmaceuticals_transactions.get_all_active_substances_sold(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        code_substance="1139",
        amm="2140050",
        libelle_substance="cymoxanil",
        fonction="fongicide",
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_bought_products_live():
    df = phytopharmaceuticals_transactions.get_all_phytopharmaceutical_products_bought(
        type_territoire="National",
        annee_min=2022,
        annee_max=2022,
        amm="2160839",
        achat_etranger="Non",
        unite=["l", "kg"],
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_unexpected_arguments():
    for func in (
        "get_all_active_substances_bought",
        "get_all_phytopharmaceutical_products_bought",
        "get_all_active_substances_sold",
        "get_all_phytopharmaceutical_products_sold",
    ):
        with pytest.raises(ValueError):
            getattr(phytopharmaceuticals_transactions, func)(dummy="blah")


def test_unexpected_values():
    for func in (
        "get_all_active_substances_bought",
        "get_all_phytopharmaceutical_products_bought",
        "get_all_active_substances_sold",
        "get_all_phytopharmaceutical_products_sold",
    ):
        with pytest.raises(ValueError):
            getattr(phytopharmaceuticals_transactions, func)(
                type_territoire="EPCI"
            )


def test_get_territory_years_combination():
    df = _get_territory_years_combination(
        {"type_territoire": "National"}, "bought"
    )
    df = pd.DataFrame(df, columns=["territory", "year"])
    assert df.territory.drop_duplicates().tolist() == [""]
    assert df.year.min() == 2013
    assert df.year.max() == date.today().year

    df = _get_territory_years_combination(
        {"type_territoire": "National"}, "sold"
    )
    df = pd.DataFrame(df, columns=["territory", "year"])
    assert df.territory.drop_duplicates().tolist() == [""]
    assert df.year.min() == 2008
    assert df.year.max() == date.today().year

    df = _get_territory_years_combination(
        {"type_territoire": "Région"}, "bought"
    )
    df = pd.DataFrame(df, columns=["territory", "year"])
    assert len(df.territory.drop_duplicates().tolist()) == 34

    df = _get_territory_years_combination(
        {"type_territoire": "Département"}, "bought"
    )
    df = pd.DataFrame(df, columns=["territory", "year"])
    assert len(df.territory.drop_duplicates().tolist()) == 102

    df = _get_territory_years_combination(
        {"type_territoire": "Zone postale"},
        filter_departements="59",
        transaction="bought",
    )
    df = pd.DataFrame(df, columns=["territory", "year"])
    assert len(df.territory.drop_duplicates().tolist()) > 200

    df = _get_territory_years_combination(
        {
            "type_territoire": "Département",
            "annee_min": 2020,
            "annee_max": 2024,
        },
        filter_regions="32",
        transaction="bought",
    )
    df = pd.DataFrame(df, columns=["territory", "year"])
    assert len(df.territory.drop_duplicates().tolist()) == 5
    assert len(df.year.drop_duplicates().tolist()) == 5
