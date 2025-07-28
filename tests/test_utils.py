#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 16:27:38 2025

Test utils functions
"""

from cl_hubeau.utils import cities_for_sage
from cl_hubeau.utils import _get_postcodes
from cl_hubeau.utils.fill_missing_fields import (
    _fill_missing_cog,
    _fill_missing_basin_subbasin,
)
from cl_hubeau.utils import _get_pynsee_geodata_latest
from cl_hubeau.utils import _get_mesh


def test_postcodes():
    assert len(_get_postcodes()) > 1000
    assert 1000 > len(_get_postcodes(code_reg=["32"])) > 500
    assert 500 > len(_get_postcodes(code_dep=["59"])) > 200


def test_sage():
    "Test SAGE retrieval"

    d = cities_for_sage()
    assert isinstance(d, dict)
    first = list(d.keys())[0]
    assert isinstance(first, str)
    assert isinstance(d[first], list)


def test_fill_territories():

    sample = _get_pynsee_geodata_latest("region", 2154)
    sample["geometry"] = sample.geometry.apply(
        lambda x: max(x.geoms, key=lambda x: x.area)
    ).centroid
    sample = sample[["geometry"]]
    columns = [
        "code_sous_bassin",
        "libelle_sous_bassin",
        "code_bassin",
        "libelle_bassin",
        "code_commune",
        "code_departement",
        "code_region",
        "libelle_commune",
        "libelle_departement",
        "libelle_region",
    ]
    for col in columns:
        sample[col] = None

    sample = _fill_missing_basin_subbasin(sample)
    sample = _fill_missing_cog(sample)

    for x in columns:
        assert not sample[x].isnull().any()


def test_mesh():
    _get_mesh()
