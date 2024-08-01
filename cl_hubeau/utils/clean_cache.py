#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clean all cache
"""

import os
import pynsee.utils

from cl_hubeau.constants import (
    DIR_CACHE,
    CACHE_NAME,
)


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
        os.unlink(cache_name)
    except FileNotFoundError:
        pass

    # Clear pynsee's cache:
    pynsee.utils.clear_all_cache()
