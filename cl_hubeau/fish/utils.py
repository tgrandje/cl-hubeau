#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for watercoastal quality inspections
"""

from datetime import date
import warnings

import deprecation
import geopandas as gpd
import pandas as pd
from tqdm import tqdm


from cl_hubeau import __version__
from cl_hubeau.fish import (
    FishSession,
)
from cl_hubeau import _config
from cl_hubeau.utils import (
    get_departements,
    get_departements_from_regions,
    prepare_kwargs_loops,
)


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations for analyses on fish

    Parameters
    ----------
    **kwargs :
        kwargs passed to FishSession.get_stations
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        DataFrame of networks (UDI)

    """

    if "code_departement" in kwargs:
        deps = [kwargs.pop("code_departement")]
    elif "code_region" in kwargs:
        deps = get_departements_from_regions(kwargs.pop("code_region"))
    else:
        deps = get_departements()

        # Split by 50-something chunks
        deps = [deps[i : i + 50] for i in range(0, len(deps), 50)]

    with FishSession() as session:
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


def get_all_observations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve observations

    Should be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_operation_min/date_operation_max are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to FishSession.get_observations
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of analysis results

    """

    if not kwargs:
        warnings.warn(
            "get_all_observations should only be used with "
            "kwargs, for instance "
            "`get_all_observations(code_departement='02')`"
        )

    # Set a loop for 6 months querying as dataset are big

    start_auto_determination = False
    if "date_operation_min" not in kwargs:
        start_auto_determination = True
        kwargs["date_operation_min"] = "1973-01-01"
    if "date_operation_max" not in kwargs:
        kwargs["date_operation_max"] = date.today().strftime("%Y-%m-%d")

    if "code_region" in kwargs:
        # let's downcast to departemental loops
        reg = kwargs.pop("code_region")
        if isinstance(reg, (list, tuple, set)):
            deps = [
                dep for r in reg for dep in get_departements_from_regions(r)
            ]
        else:
            deps = get_departements_from_regions(reg)
        kwargs["code_departement"] = deps

    if "code_departement" in kwargs:
        deps = [kwargs.pop("code_departement")]
    elif "code_region" in kwargs:
        deps = get_departements_from_regions(kwargs.pop("code_region"))
    else:
        deps = get_departements()

    desc = "querying 6m/6m" + (
        " & dep/dep" if "code_departement" in kwargs else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_operation_min",
        "date_operation_max",
        kwargs,
        start_auto_determination,
        split_months=6,
    )

    with FishSession() as session:

        results = [
            session.get_observations(**kwargs, **kw_loop)
            for kw_loop in tqdm(
                kwargs_loop,
                desc=desc,
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
        results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
        results = pd.concat(results, ignore_index=True)

        return results


def get_all_operations(**kwargs) -> pd.DataFrame:
    """
    Retrieve operations from multiple stations.

    Parameters
    ----------
    codes_entites : list of stations
        List of site
    **kwargs :
        kwargs passed to FishSession.get_stations
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    start_auto_determination = False
    if "date_operation_min" not in kwargs:
        start_auto_determination = True
        kwargs["date_operation_min"] = "1973-01-01"
    if "date_operation_max" not in kwargs:
        kwargs["date_operation_max"] = date.today().strftime("%Y-%m-%d")

    if "code_region" in kwargs:
        # let's downcast to departemental loops
        reg = kwargs.pop("code_region")
        if isinstance(reg, (list, tuple, set)):
            deps = [
                dep for r in reg for dep in get_departements_from_regions(r)
            ]
        else:
            deps = get_departements_from_regions(reg)
        kwargs["code_departement"] = deps

    if "code_departement" in kwargs:
        deps = [kwargs.pop("code_departement")]
    elif "code_region" in kwargs:
        deps = get_departements_from_regions(kwargs.pop("code_region"))
    else:
        deps = get_departements()

    desc = "querying 6m/6m" + (
        " & dep/dep" if "code_departement" in kwargs else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_operation_min",
        "date_operation_max",
        kwargs,
        start_auto_determination,
        split_months=6,
    )

    with FishSession() as session:

        results = [
            session.get_operations(**kwargs, **kw_loop)
            for kw_loop in tqdm(
                kwargs_loop,
                desc=desc,
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


def get_all_indicators(**kwargs) -> pd.DataFrame:
    """
    Retrieve indicators from multiple stations.

    Parameters
    ----------
    codes_entites : list of stations
        List of site
    **kwargs :
        kwargs passed to FishSession.get_stations
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    deps = get_departements()
    with FishSession() as session:
        results = [
            session.get_indicators(
                code_departement=dep, format="geojson", **kwargs
            )
            for dep in tqdm(
                deps,
                desc="querying entite/entite",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


if __name__ == "__main__":

    df = get_all_observations(code_departement="02")
