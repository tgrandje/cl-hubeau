#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for hydrometry consumption
"""

import geopandas as gpd
import pandas as pd

# import pebble
from tqdm import tqdm

from cl_hubeau.hydrometry.hydrometry_scraper import HydrometrySession
from cl_hubeau import _config
from cl_hubeau.utils import get_departements


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrometrySession.get_stations (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_departement`
        as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of piezometers

    """

    deps = get_departements().unique().tolist()

    with HydrometrySession() as session:
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
        results["code_station"]
        results = results.drop_duplicates("code_station")
    except KeyError:
        pass
    return results


def get_all_sites(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all sites from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrometrySession.get_sites (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_departement`
        as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of piezometers

    """

    deps = get_departements().unique().tolist()

    with HydrometrySession() as session:
        results = [
            session.get_sites(code_departement=dep, format="geojson", **kwargs)
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
        results["code_site"]
        results = results.drop_duplicates("code_site")
    except KeyError:
        pass
    return results


def get_observations(codes_entites: list, **kwargs) -> pd.DataFrame:
    """
    Retrieve observations from multiple sites/stations.

    Use an inner loop for multiple piezometers to avoid reaching 20k results
    threshold from hub'eau API.

    Parameters
    ----------
    codes_entites : list
        List of site or station codes for hydrometry
    **kwargs :
        kwargs passed to PiezometrySession.get_chronicles (hence mostly
        intended for hub'eau API's arguments). Do not use `code_entite` as they
        are set by the current function.

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    with HydrometrySession() as session:
        results = [
            session.get_observations(code_entite=code, **kwargs)
            for code in tqdm(
                codes_entites,
                desc="querying entite/entite",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


def get_realtime_observations(codes_entites: list, **kwargs) -> pd.DataFrame:
    """
    Retrieve realtimes observations from multiple sites/stations.
    Uses a reduced timeout for cache expiration.

    Parameters
    ----------
    codes_entites : list
        List of site or station codes for hydrometry
    **kwargs :
        kwargs passed to PiezometrySession.get_chronicles (hence mostly
        intended for hub'eau API's arguments). Do not use `code_entite` as they
        are set by the current function.

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    with HydrometrySession(
        expire_after=_config["DEFAULT_EXPIRE_AFTER_REALTIME"]
    ) as session:
        results = [
            session.get_realtime_observations(code_entite=code, **kwargs)
            for code in tqdm(
                codes_entites,
                desc="querying entite/entite",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results
