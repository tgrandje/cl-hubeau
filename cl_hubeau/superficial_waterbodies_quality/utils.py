#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for superficial waterbodies quality inspections
"""

from datetime import date
import warnings

import deprecation
import geopandas as gpd
import pandas as pd
from tqdm import tqdm


from cl_hubeau import __version__
from cl_hubeau.superficial_waterbodies_quality import (
    SuperficialWaterbodiesQualitySession,
)
from cl_hubeau import _config
from cl_hubeau.utils import (
    get_departements,
    get_departements_from_regions,
    prepare_kwargs_loops,
)


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations for physical/chemical analyses on superficial
    waterbodies

    Use a loop to avoid reaching 20k results threshold. Do not use
    `code_departement` or `format` as they are set by the current function.

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_stations
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        DataFrame of networks (UDI) /cities coverage

    """

    deps = get_departements()

    # Split by 20-something chunks
    deps = [deps[i : i + 20] for i in range(0, len(deps), 20)]

    with SuperficialWaterbodiesQualitySession() as session:
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
    return results


def get_all_operations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve operations for measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_operations
        (hence mostly intended for hub'eau API's arguments).
        Do not use `format` which is used by this function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of operations

    """

    if not kwargs:
        warnings.warn(
            "get_all_operations should only be used with "
            "kwargs, for instance `get_operations(code_departement='02')`"
        )

    # Set a loop for yearly querying as dataset are big

    start_auto_determination = False
    if "date_debut_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_debut_prelevement"] = "1960-01-01"
    if "date_fin_prelevement" not in kwargs:
        kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

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

    desc = "querying 6m/6m" + (
        " & dep/dep" if "code_departement" in kwargs else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_debut_prelevement",
        "date_fin_prelevement",
        kwargs,
        start_auto_determination,
    )

    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_operations(
                format="geojson",
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
    results = pd.concat(results, ignore_index=True)
    return results


def get_all_environmental_conditions(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve environmental conditions for measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_environmental_conditions
        (hence mostly intended for hub'eau API's arguments).
        Do not use `format` which is used by this function.

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of environmental conditions

    """

    if not kwargs:
        warnings.warn(
            "get_all_environmental_conditions should only be used with "
            "kwargs, for instance "
            "`get_all_environmental_conditions(code_department='02')`"
        )

    # Set a loop for yearly querying as dataset are big

    start_auto_determination = False
    if "date_debut_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_debut_prelevement"] = "1960-01-01"
    if "date_fin_prelevement" not in kwargs:
        kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

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

    desc = "querying year/year" + (
        " & dep/dep" if "code_departement" in kwargs else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_debut_prelevement",
        "date_fin_prelevement",
        kwargs,
        start_auto_determination,
    )

    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_environmental_conditions(
                format="geojson", **kwargs, **kw_loop
            )
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


@deprecation.deprecated(
    deprecated_in="0.6.0",
    removed_in="1.0",
    current_version=__version__,
    details="Please use `get_all_analyses` instead.",
)
def get_all_analysis(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve analyses results from measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_analyses
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of analyses results

    """
    return get_all_analyses(**kwargs)


def get_all_analyses(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve analyses results from measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_analyses
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of analyses results

    """

    if not kwargs:
        warnings.warn(
            "get_all_analyses should only be used with "
            "kwargs, for instance "
            "`get_all_analyses(code_department='02')`"
        )

    # Set a loop for yearly querying as dataset are big

    start_auto_determination = False
    if "date_debut_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_debut_prelevement"] = "1960-01-01"
    if "date_fin_prelevement" not in kwargs:
        kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

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

    desc = "querying year/year" + (
        " & dep/dep" if "code_departement" in kwargs else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_debut_prelevement",
        "date_fin_prelevement",
        kwargs,
        start_auto_determination,
    )

    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_analyses(format="geojson", **kwargs, **kw_loop)
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
