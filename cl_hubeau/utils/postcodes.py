#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenienence module retrieving available postcodes
"""

from io import BytesIO
import os
from typing import Union

import diskcache
import numpy as np
import pandas as pd
from requests import Session

from cl_hubeau.constants import DIR_CACHE, DISKCACHE
from cl_hubeau.config import _config
from cl_hubeau.utils.cities_deps_regions import _get_pynsee_geolist_cities

cache = diskcache.Cache(os.path.join(DIR_CACHE, DISKCACHE))


@cache.memoize(
    tag="postcodes", expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds()
)
def _get_postcodes(
    code_reg: Union[str, list, tuple, None] = None,
    code_dep: Union[str, list, tuple, None] = None,
) -> np.array:
    """
    Retrieve available postcodes from La Poste.

    This function is cached locally.

    Parameters
    ----------
    code_reg : Union[str, list, tuple, None]
        If given, will filter the output to keep only postcodes from the set
        region(s). This argument is mutually exclusive with code_dep.
    code_dep : Union[str, list, tuple, None]
        if given, will filter the output to keep only postcodes from the set
        departement(s). This argument is mutually exclusive with code_dep.

    Returns
    -------
    postcodes : np.array
        Array of postcodes

    Examples
    -------
    >>> get_postcodes()
    array(['01400', '01640', '01500', ..., '98818', '98799', '98000'],
          dtype=object)

    >>> get_postcodes(code_dep=["02", "60", "62"])
    array(['02300', '02800', '02200', ..., '62520', '62930', '62820'],
          dtype=object)

    >>> get_postcodes(code_reg="32")
    array(['02300', '02800', '02200', ..., '80780', '80880', '80650'],
          dtype=object)

    """

    if code_reg and code_dep:
        raise ValueError("code_reg & code_dep are mutually exclusive")

    url = (
        "https://datanova.laposte.fr/data-fair/"
        "api/v1/datasets/laposte-hexasmal/raw"
    )

    session = Session()
    session.proxies.update(
        {
            "https": os.environ.get("https_proxy", None),
            "http": os.environ.get("http_proxy", None),
        }
    )

    r = session.get(url)
    obj = BytesIO(r.content)
    df = pd.read_csv(obj, sep=";", encoding="cp1252", dtype="str")

    df = df[["#Code_commune_INSEE", "Code_postal"]].rename(
        {"#Code_commune_INSEE": "code_commune", "Code_postal": "code_postal"},
        axis=1,
    )
    cols = ["CODE", "CODE_DEP", "CODE_REG"]
    cities = _get_pynsee_geolist_cities().loc[:, cols]
    df = df.merge(cities, left_on="code_commune", right_on="CODE")
    df = df.drop(["CODE", "code_commune"], axis=1)

    if code_reg:
        if isinstance(code_reg, str):
            code_reg = [code_reg]
        df = df.query(f"CODE_REG.isin({code_reg})")
    elif code_dep:
        if isinstance(code_dep, str):
            code_dep = [code_dep]
        df = df.query(f"CODE_DEP.isin({code_dep})")

    postcodes = df.code_postal.unique()
    return postcodes
