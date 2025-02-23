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

    with GroundWaterQualitySession() as session:

        deps = get_departements()
        results = [
            session.get_stations(
                num_departement=dep, format="geojson", **kwargs
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

    if "code_region" in kwargs:
        # let's downcast to departemental loops
        reg = kwargs.pop("code_region")
        if isinstance(reg, (list, tuple, set)):
            deps = [
                dep for r in reg for dep in get_departements_from_regions(r)
            ]
        else:
            deps = get_departements_from_regions(reg)
        kwargs["num_departement"] = deps

    desc = "querying 6m/6m" + (
        " & dep/dep" if "num_departement" in kwargs else ""
    )

    kwargs_loop = prepare_kwargs_loops(
        "date_debut_prelevement",
        "date_fin_prelevement",
        kwargs,
        start_auto_determination,
        split_months=6,
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
    results = pd.concat(results, ignore_index=True)
    return results
