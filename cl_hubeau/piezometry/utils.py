#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for piezometry consumption
"""

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.piezometry.piezometry_scraper import PiezometrySession
from cl_hubeau import _config
from cl_hubeau.utils import get_departements


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all piezometers from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to PiezometrySession.get_stations (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_departement`
        as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of piezometers

    """

    with PiezometrySession() as session:

        deps = get_departements().unique().tolist()
        results = [
            session.get_stations(
                code_departement=dep, format="geojson", **kwargs
            )
            for dep in tqdm(
                deps,
                desc="querying dep/dep",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = gpd.pd.concat(results, ignore_index=True)
    try:
        results["code_bss"]
        results = results.drop_duplicates("code_bss")
    except KeyError:
        pass
    return results


def get_chronicles(codes_bss: list, **kwargs) -> pd.DataFrame:
    """
    Retrieve chronicles from multiple piezometers.

    Use an inner loop for multiple piezometers to avoid reaching 20k results
    threshold from hub'eau API.

    Parameters
    ----------
    codes_bss : list
        List of code_bss codes for piezometers
    **kwargs :
        kwargs passed to PiezometrySession.get_chronicles (hence mostly
        intended for hub'eau API's arguments). Do not use `code_bss` as they
        are set by the current function.

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    with PiezometrySession() as session:
        results = [
            session.get_chronicles(code_bss=code, **kwargs)
            for code in tqdm(
                codes_bss,
                desc="querying piezo/piezo",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


def get_realtime_chronicles(codes_bss: list, **kwargs) -> pd.DataFrame:
    """
    Retrieve realtimes chronicles from multiple piezometers.
    Uses a reduced timeout for cache expiration.

    Parameters
    ----------
    codes_bss : list
        List of code_bss codes for piezometers
    **kwargs :
        kwargs passed to PiezometrySession.get_chronicles (hence mostly
        intended for hub'eau API's arguments). Do not use `code_bss` as they
        are set by the current function.

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    with PiezometrySession(
        expire_after=_config["DEFAULT_EXPIRE_AFTER_REALTIME"]
    ) as session:
        results = [
            session.get_realtime_chronicles(code_bss=code, **kwargs)
            for code in tqdm(
                codes_bss,
                desc="querying piezo/piezo",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


if __name__ == "__main__":

    df = get_all_stations()
