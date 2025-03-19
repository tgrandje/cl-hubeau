#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for water services indicators.
"""

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.water_services.water_services_scraper import WaterServicesSession
from cl_hubeau import _config
from cl_hubeau.utils import get_cities

def get_all_communes(**kwargs) -> gpd.GeoDataFrame:
    """
    Gets the services for every french city.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WaterServicesSession.get_communes (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_commune`
        as they are set by the current function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of water services indicators for every french city.

    """
    with WaterServicesSession() as session :
        cities = get_cities()
        results = [
            session.get_communes(
                code_commune=city, format="geojson", **kwargs
            )
            for city in tqdm(
                cities,
                desc="querying city/city",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = gpd.pd.concat(results, ignore_index=True)
    try:
        results["code_commune_insee"]
        results = results.drop_duplicates("code_commune_insee")
    except KeyError:
        pass
    return results

def get_all_services(**kwargs) -> pd.DataFrame:
    """
    Gets the services for every french city.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WaterServicesSession.get_services (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_commune`
        as they are set by the current function.

    Returns
    -------
    results : pd.DataFrame
        DataFrame of water services indicators for every service.

    """
    with WaterServicesSession() as session:
        cities = get_cities()
        results = [
            session.get_services(
                code_commune=city, **kwargs
            )
            for city in tqdm(
                cities,
                desc="querying cities",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    try:
        results["code_service"]
        results = results.drop_duplicates("code_service")
    except KeyError:
        pass
    return results