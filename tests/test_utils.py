#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 16:27:38 2025

Test utils functions
"""

from cl_hubeau.utils import cities_for_sage
from cl_hubeau.utils import _get_postcodes


def test_sage():
    "Test SAGE retrieval"

    d = cities_for_sage()
    assert isinstance(d, dict)
    first = list(d.keys())[0]
    assert isinstance(first, str)
    assert isinstance(d[first], list)


def test_postcodes():
    assert len(_get_postcodes()) > 1000
    assert 1000 > len(_get_postcodes(code_reg=["32"])) > 500
    assert 500 > len(_get_postcodes(code_dep=["59"])) > 200
