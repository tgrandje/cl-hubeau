#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Get list of cities' and departements' codes
"""

import os
from pathlib import Path

from pynsee import get_area_list
from pynsee.utils.init_conn import init_conn

from cl_hubeau.utils.clean_cache import clean_all_cache


def init_pynsee_connection():
    """
    Initiate an INSEE API connection with tokens and proxies.
    """
    home = str(Path.home())
    pynsee_credentials_file = os.path.join(home, "pynsee_credentials.csv")
    if not os.path.exists(pynsee_credentials_file):
        clean_all_cache()
        keys = ["insee_key", "insee_secret", "http_proxy", "https_proxy"]
        kwargs = {x: os.environ[x] for x in keys if x in os.environ}
        init_conn(**kwargs)


def get_cities():
    init_pynsee_connection()
    cities = get_area_list("communes", "*", silent=True)
    return cities["CODE"]


def get_departements():
    init_pynsee_connection()
    deps = get_area_list("departements", "*", silent=True)
    return deps["CODE"]
