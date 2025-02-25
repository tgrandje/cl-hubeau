#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clean all cache
"""

import os
import pynsee.utils
from pynsee.utils._clean_insee_folder import _clean_insee_folder

from cl_hubeau.constants import (
    DIR_CACHE,
    CACHE_NAME,
)
from .hydro_perimeters_queries import cache


def clean_all_cache(cache_name: str = os.path.join(DIR_CACHE, CACHE_NAME)):
    """
    Clean http(s) cache, then pynsee's cache

    Parameters
    ----------
    cache_name : str, optional
        Cache name. The default is os.path.join(DIR_CACHE, CACHE_NAME).

    Returns
    -------
    None.

    """
    try:
        os.remove(cache_name)
    except FileNotFoundError:
        pass

    cache.clear(retry=True)

    # Clear pynsee's cache:
    pynsee.utils.clear_all_cache()
    _clean_insee_folder()
