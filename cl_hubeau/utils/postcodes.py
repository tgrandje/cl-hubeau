#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenienence module retrieving available postcodes
"""

from io import BytesIO
import os

import diskcache
import numpy as np
import pandas as pd
from requests import Session

from cl_hubeau.constants import DIR_CACHE, DISKCACHE
from cl_hubeau.config import _config

cache = diskcache.Cache(os.path.join(DIR_CACHE, DISKCACHE))


@cache.memoize(
    tag="postcodes", expire=_config["DEFAULT_EXPIRE_AFTER"].total_seconds()
)
def get_postcodes() -> np.array:
    """
    Retrieve all available postcodes from La Poste.

    Returns
    -------
    postcodes : np.array
        Array of postcodes

    Examples
    -------
    >>> get_postcodes()
    array(['01400', '01640', '01500', ..., '98818', '98799', '98000'],
          dtype=object)

    """
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
    postcodes = df.code_postal.unique()
    return postcodes


if __name__ == "__main__":
    postcodes = get_postcodes()
