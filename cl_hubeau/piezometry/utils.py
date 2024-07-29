#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 09:40:51 2024
"""

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.piezometry.piezometry_scraper import PiezometrySession
from cl_hubeau.constants import DEPARTEMENTS


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
        results = [
            session.get_stations(
                code_departement=dep, format="geojson", **kwargs
            )
            for dep in tqdm(DEPARTEMENTS, desc="querying dep/dep", leave=False)
        ]
    results = gpd.pd.concat(results, ignore_index=True)
    try:
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
                codes_bss, desc="querying piezo/piezo", leave=False
            )
        ]
    results = pd.concat(results, ignore_index=True)
    return results


def get_realtime_chronicles(codes_bss: list, **kwargs) -> pd.DataFrame:
    """
    Retrieve realtimes chronicles from multiple piezometers.

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
            session.get_chronicles_real_time(code_bss=code, **kwargs)
            for code in tqdm(
                codes_bss, desc="querying piezo/piezo", leave=False
            )
        ]
    results = pd.concat(results, ignore_index=True)
    return results
