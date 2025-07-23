#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for hydrometry consumption
"""

import geopandas as gpd
import pandas as pd
import warnings

from tqdm import tqdm

from cl_hubeau.hydrometry.hydrometry_scraper import HydrometrySession
from cl_hubeau import _config
from cl_hubeau.utils import get_departements, get_departements_from_regions


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations from France.

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrometrySession.get_stations (hence mostly intended
        for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of piezometers

    """

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    elif any(
        x in kwargs
        for x in ("code_commune_station", "code_site", "code_station")
    ):
        deps = [""]
    else:
        deps = get_departements()

    kwargs["format"] = kwargs.get("format", "geojson")

    with HydrometrySession() as session:
        results = [
            session.get_stations(code_departement=dep, **kwargs)
            for dep in tqdm(
                deps,
                desc="querying dep/dep",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()
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
        for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of piezometers

    """

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    elif any(
        x in kwargs for x in ("code_commune_site", "code_site", "code_station")
    ):
        deps = [""]
    else:
        deps = get_departements()

    kwargs["format"] = kwargs.get("format", "geojson")

    with HydrometrySession() as session:
        results = [
            session.get_sites(code_departement=dep, **kwargs)
            for dep in tqdm(
                deps,
                desc="querying dep/dep",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()

    results = gpd.pd.concat(results, ignore_index=True)
    try:
        results["code_site"]
        results = results.drop_duplicates("code_site")
    except KeyError:
        pass
    return results


def get_observations(**kwargs) -> pd.DataFrame:
    """
    Retrieve observations from multiple sites/stations.

    Use an inner loop for multiple piezometers to avoid reaching 20k results
    threshold from hub'eau API.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use either a `code_region`, `code_departement` or `code_commune`
      argument to query the results on a given region/departement/commune.
      Those arguments are mutually exclusive with `code_entite`.

    Parameters
    ----------
    **kwargs :
        kwargs passed to PiezometrySession.get_chronicles (hence mostly
        intended for hub'eau API's arguments).

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    if "codes_entites" in kwargs:
        msg = (
            "`codes_entites` is deprecated and will be removed in a future "
            "version, please use `code_entite` instead"
        )
        warnings.warn(msg, category=FutureWarning, stacklevel=2)
        kwargs["code_entite"] = kwargs.pop("codes_entites")

    if "code_entite" in kwargs:
        codes_entites = kwargs.pop("code_entite")
        if isinstance(codes_entites, str):
            codes_entites = codes_entites.split(",")

        conflicts = ["code_region", "code_departement", "code_commune"]
        if any(x for x in conflicts if x in kwargs):
            raise ValueError(
                "only one argument allowed among either 'code_commune', "
                "'code_departement', 'code_region' in the one hand AND "
                "'code_entite' in the other hand."
            )

    else:

        kwargs_entites = {"format": "json"}
        if "code_region" in kwargs:
            code_region = kwargs.pop("code_region")
            deps = get_departements_from_regions(code_region)
            kwargs_entites["code_departement"] = deps
        elif "code_departement" in kwargs:
            deps = kwargs.pop("code_departement")
            kwargs_entites["code_departement"] = deps
        elif "code_commune" in kwargs:
            kwargs_entites["code_commune"] = kwargs.pop("code_commune")

        # retrieve code_entites
        if "code_commune" in kwargs_entites:
            kwargs_entites["code_commune_station"] = kwargs_entites.pop(
                "code_commune"
            )
        stations = get_all_stations(fields=["code_station"], **kwargs_entites)

        if "code_commune_station" in kwargs_entites:
            kwargs_entites["code_commune_site"] = kwargs_entites.pop(
                "code_commune_station"
            )
        sites = get_all_sites(fields=["code_site"], **kwargs_entites)

        codes_entites = list(
            set(
                stations["code_station"].tolist() + sites["code_site"].tolist()
            )
        )

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
    if not results:
        return pd.DataFrame()
    results = pd.concat(results, ignore_index=True)
    return results


def get_realtime_observations(**kwargs) -> pd.DataFrame:
    """
    Retrieve realtimes observations from multiple sites/stations.
    Uses a reduced timeout for cache expiration.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use either a `code_region`, `code_departement` or `code_commune`
      argument to query the results on a given region/departement/commune.
      Those arguments are mutually exclusive with `code_entite`.

    Parameters
    ----------
    **kwargs :
        kwargs passed to PiezometrySession.get_chronicles (hence mostly
        intended for hub'eau API's arguments).

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    if "codes_entites" in kwargs:
        msg = (
            "`codes_entites` is deprecated and will be removed in a future "
            "version, please use `code_entite` instead"
        )
        warnings.warn(msg, category=FutureWarning, stacklevel=2)
        kwargs["code_entite"] = kwargs.pop("codes_entites")

    if "code_entite" in kwargs:
        codes_entites = kwargs.pop("code_entite")
        if isinstance(codes_entites, str):
            codes_entites = codes_entites.split(",")

        conflicts = ["code_region", "code_departement", "code_commune"]
        if any(x for x in conflicts if x in kwargs):
            raise ValueError(
                "only one argument allowed among either 'code_commune', "
                "'code_departement', 'code_region' in the one hand AND "
                "'code_entite' in the other hand."
            )

    else:

        kwargs_entites = {"format": "json"}
        if "code_region" in kwargs:
            code_region = kwargs.pop("code_region")
            deps = get_departements_from_regions(code_region)
            kwargs_entites["code_departement"] = deps
        elif "code_departement" in kwargs:
            deps = kwargs.pop("code_departement")
            kwargs_entites["code_departement"] = deps
        elif "code_commune" in kwargs:
            kwargs_entites["code_commune"] = kwargs.pop("code_commune")

        # retrieve code_entites
        if "code_commune" in kwargs_entites:
            kwargs_entites["code_commune_station"] = kwargs_entites.pop(
                "code_commune"
            )
        stations = get_all_stations(fields=["code_station"], **kwargs_entites)

        if "code_commune_station" in kwargs_entites:
            kwargs_entites["code_commune_site"] = kwargs_entites.pop(
                "code_commune_station"
            )
        sites = get_all_sites(fields=["code_site"], **kwargs_entites)

        codes_entites = list(
            set(
                stations["code_station"].tolist() + sites["code_site"].tolist()
            )
        )

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
    if not results:
        return pd.DataFrame()
    results = pd.concat(results, ignore_index=True)
    return results
