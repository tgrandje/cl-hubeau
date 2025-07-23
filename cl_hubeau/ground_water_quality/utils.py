#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for ground water quality consumption
"""

from datetime import date
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
import warnings

from cl_hubeau.ground_water_quality import GroundWaterQualitySession
from cl_hubeau import _config
from cl_hubeau.utils import (
    get_departements,
    get_departements_from_regions,
    prepare_kwargs_loops,
)


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations from France.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use a code_region argument to query the results on a given region

    Parameters
    ----------
    **kwargs :
        kwargs passed to PiezometrySession.get_stations (hence mostly intended
        for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of piezometers

    """

    if "code_departement" in kwargs:
        warnings.warn(
            "kwargs code_departement was found, perhaps you meant "
            "num_departement ?"
        )

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "num_departement" in kwargs:
        deps = kwargs.pop("num_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    elif any(x in kwargs for x in ("code_commune", "bss_id")):
        deps = [""]
    else:
        deps = get_departements()

    with GroundWaterQualitySession() as session:

        kwargs["format"] = kwargs.get("format", "geojson")

        results = [
            session.get_stations(num_departement=dep, **kwargs)
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
        results["code_bss"]
        results = results.drop_duplicates("code_bss")
    except KeyError:
        pass
    return results


def get_all_analyses(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve analyses for all/multiple qualitometers.

    Should be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to GroundWaterQualitySession.get_analysis
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of analysis results

    """

    if not kwargs:
        warnings.warn(
            "get_all_analysis should only be used with "
            "kwargs, for instance "
            "`get_all_analysis(num_departement='02')`"
        )

    # Set a loop for 6 months querying as dataset are big

    start_auto_determination = False
    if "date_debut_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_debut_prelevement"] = "1960-01-01"
    if "date_fin_prelevement" not in kwargs:
        kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

    if "code_commune" in kwargs:
        warnings.warn(
            "kwargs code_commune was found, perhaps you meant "
            "code_insee_actuel ?"
        )

    if "code_departement" in kwargs:
        warnings.warn(
            "kwargs code_departement was found, perhaps you meant "
            "num_departement ?"
        )

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "num_departement" in kwargs:
        deps = kwargs.pop("num_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    elif any(x in kwargs for x in ("code_insee_actuel", "bss_id")):
        deps = [""]
    else:
        deps = get_departements()

    kwargs["num_departement"] = deps

    desc = "querying 6m/6m" + (
        " & dep/dep" if "num_departement" != [""] else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_debut_prelevement",
        "date_fin_prelevement",
        kwargs,
        start_auto_determination,
    )

    with GroundWaterQualitySession() as session:

        results = [
            session.get_analyses(**kwargs, **kw_loop)
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
    results = pd.concat(results, ignore_index=True)
    return results
