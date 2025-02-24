#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 16:27:38 2025

Test utils functions
"""

from cl_hubeau.utils import cities_for_sage


def test_sage():
    "Test SAGE retrieval"

    d = cities_for_sage()
    assert isinstance(d, dict)
    first = list(d.keys())[0]
    assert isinstance(first, str)
    assert isinstance(d[first], list)
