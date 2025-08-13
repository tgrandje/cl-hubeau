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
from cl_hubeau.utils import get_departements_from_regions, get_regions, prepare_kwargs_loops
from datetime import datetime

def get_all_communes(code_region=None, **kwargs) -> gpd.GeoDataFrame:
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
    if(not kwargs.get("code_commune")):
        if code_region :
            deps = get_departements_from_regions(code_region)
        elif kwargs.get("code_departement") :
            deps = kwargs.get("code_departement")
        else :
            deps = get_departements_from_regions(get_regions(True))
    with WaterServicesSession() as session :
        if(kwargs.get("code_commune")) :
            results = [session.get_communes(
                code_commune=kwargs.get("code_commune")
                )]
        else:
            results = [
                session.get_communes(
                    code_departement=get_departements_from_regions(dep), detail_service=True, format="geojson", **kwargs
                )
                for dep in tqdm(
                    deps,
                    desc="querying regions for communes",
                    leave=_config["TQDM_LEAVE"],
                    position=tqdm._get_free_pos(),
                )
            ]
    results = [x[0].dropna(axis=1, how="all") for x in results if not x[0].empty]
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
        regions = get_regions(True)
        results = []
        for reg in tqdm(
            regions, desc="Querying regions for services",
            position=0
        ):
            for dep in tqdm(
                get_departements_from_regions(reg),
                desc="Querying departements from regions",
                leave=False,
                position=1
            ):
                result = session.get_services(code_departement=dep, **kwargs)
                results.append(result)
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    try:
        results["code_service"]
        results = results.drop_duplicates("code_service")
    except KeyError:
        pass
    return results

def get_all_indicators(**kwargs) -> pd.DataFrame:
    """
    Gets a given indicator value for every recorded french city at any time.

    Parameters
    ----------
    **kwargs :
        kwargs passed to WaterServicesSession.get_indicators (hence mostly intended
        for hub'eau API's arguments). Do not use `format` or `code_commune`
        as they are set by the current function.

    Returns
    -------
    results : pd.DataFrame
        DataFrame of water services indicators for every service.

    """
    with WaterServicesSession() as session:
        years = range(2000, datetime.now().year)
        print(years)
        results = [
            session.get_indicators(
                annee = year,
                **kwargs,
            )
            for year in tqdm(
                years,
                desc="querying indicators for every year",
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