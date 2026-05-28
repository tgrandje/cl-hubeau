#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for hydrometry consumption
"""

from datetime import date
import warnings

from dateutil.relativedelta import relativedelta
import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.hydrometry.hydrometry_scraper import HydrometrySession
from cl_hubeau import _config
from cl_hubeau.utils import (
    get_departements,
    get_departements_from_regions,
    _prepare_kwargs,
)

PROPAGATION_OK = {
    "bbox",
    "code_commune_site",
    "code_cours_eau",
    "code_departement",
    "code_region",
    "code_site",
    "code_troncon_hydro_site",
    "code_zone_hydro_site",
    "distance",
    "latitude",
    "longitude",
    "libelle_cours_eau",
    "libelle_site",
    "code_entite",
    "code_commune",
}


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
                desc="querying dep/dep for stations",
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
                desc="querying dep/dep for sites",
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


def _get_entities(**kwargs) -> pd.DataFrame:
    """
    Inner function allowing retrieval of both sites and stations. This is used
    to prepare observations retrieval, which are allowing any one of those as
    'code_entite'.

    Parameters
    ----------
    **kwargs : any arguments allowed by both sites and stations endpoints

    Returns
    -------
    pd.DataFrame
        This DataFrame contains only one column of "primary keys"
    """

    # hack : remove fields(code_entite) & fill_values (set by _prepare_kwargs)
    del kwargs["fields"]
    del kwargs["fill_values"]

    if "code_entite" in kwargs:
        kwargs["code_station"] = kwargs.pop("code_entite")

    if "code_commune" in kwargs:
        kwargs["code_commune_station"] = kwargs.pop("code_commune")

    stations = get_all_stations(fields=["code_station"], **kwargs)

    if "code_commune_station" in kwargs:
        kwargs["code_commune_site"] = kwargs.pop("code_commune_station")

    if "code_station" in kwargs:
        kwargs["code_site"] = kwargs.pop("code_station")

    sites = get_all_sites(fields=["code_site"], **kwargs)

    pk = "code_entite"

    entities = []
    if not stations.empty:
        entities.append(stations.rename(columns={"code_station": pk})[[pk]])
    if not sites.empty:
        entities.append(sites.rename(columns={"code_site": pk})[[pk]])

    entities = (
        pd.concat(entities, ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return entities


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
        kwargs["code_entite"] = codes_entites

        conflicts = ["code_region", "code_departement", "code_commune"]
        if any(x for x in conflicts if x in kwargs):
            raise ValueError(
                "only one argument allowed among either 'code_commune', "
                "'code_departement', 'code_region' in the one hand AND "
                "'code_entite' in the other hand."
            )

    # forcer le json par défaut
    kwargs["format"] = kwargs.get("format", "json")

    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=100,
        months=6,
        date_start_label="date_debut_obs_elab",
        date_end_label="date_fin_obs_elab",
        start_date="1900-01-01",
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_entite",
        get_entities_func=_get_entities,
    )

    desc = "querying 6m/6m & 100 entities / 100 entities"
    with HydrometrySession() as session:
        results = [
            session.get_observations(
                **kwargs,
                **kw_loop,
            )
            for kw_loop in tqdm(
                kwargs_loop,
                desc=desc,
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]

    if not results:
        return pd.DataFrame()

    results = pd.concat(results, ignore_index=True).drop_duplicates()
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
        kwargs["code_entite"] = codes_entites

        conflicts = ["code_region", "code_departement", "code_commune"]
        if any(x for x in conflicts if x in kwargs):
            raise ValueError(
                "only one argument allowed among either 'code_commune', "
                "'code_departement', 'code_region' in the one hand AND "
                "'code_entite' in the other hand."
            )

    # force json as default
    kwargs["format"] = kwargs.get("format", "json")

    # force default to prevent _prepare_kwargs from initializing to 1900-01-01
    kwargs["date_debut_obs"] = kwargs.get(
        "date_debut_obs",
        (date.today() - relativedelta(months=1)).strftime("%Y-%m-%d"),
    )

    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=100,
        months=1,
        date_start_label="date_debut_obs",
        date_end_label="date_fin_obs",
        start_date=kwargs["date_debut_obs"],
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_entite",
        get_entities_func=_get_entities,
    )

    desc = "querying 1m/1m & 100 entities / 100 entities"
    with HydrometrySession() as session:
        results = [
            session.get_realtime_observations(
                **kwargs,
                **kw_loop,
            )
            for kw_loop in tqdm(
                kwargs_loop,
                desc=desc,
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]

    if not results:
        return pd.DataFrame()

    results = pd.concat(results, ignore_index=True).drop_duplicates()
    return results
