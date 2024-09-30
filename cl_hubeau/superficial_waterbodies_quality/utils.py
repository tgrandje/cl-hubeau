#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for superficial waterbodies quality inspections
"""

from datetime import date, datetime
import warnings

import geopandas as gpd
import pandas as pd
from tqdm import tqdm


from cl_hubeau.superficial_waterbodies_quality import (
    SuperficialWaterbodiesQualitySession,
)
from cl_hubeau import _config
from cl_hubeau.utils import get_departements


def get_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations for physical/chemical analysis on superficial
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


def get_operations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve operations for measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_min_prelevement/date_max_prelevement are
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
            "kwargs, for instance `get_all_operations(code_departement='02')`"
        )

    # Set a loop for yearly querying as dataset are big

    start_auto_determination = False
    if "date_debut_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_debut_prelevement"] = "1960-01-01"
    if "date_fin_prelevement" not in kwargs:
        kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

    ranges = pd.date_range(
        start=datetime.strptime(
            kwargs.pop("date_debut_prelevement"), "%Y-%m-%d"
        ).date(),
        end=datetime.strptime(
            kwargs.pop("date_fin_prelevement"), "%Y-%m-%d"
        ).date(),
    )
    dates = pd.Series(ranges).to_frame("date")
    dates["year"] = dates["date"].dt.year
    dates = dates.groupby("year")["date"].agg(["min", "max"])
    for d in "min", "max":
        dates[d] = dates[d].dt.strftime("%Y-%m-%d")
    if start_auto_determination:
        dates = pd.concat(
            [
                dates,
                pd.DataFrame([{"min": "1900-01-01", "max": "1960-01-01"}]),
            ],
            ignore_index=False,
        ).sort_index()

    dates = dates.values.tolist()

    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_operations(
                date_debut_prelevement=date_min,
                date_fin_prelevement=date_max,
                format="geojson",
                **kwargs,
            )
            for date_min, date_max in tqdm(
                dates,
                desc="querying year/year",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


def get_environmental_conditions(**kwargs) -> gpd.GeoDataFrame:
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

    ranges = pd.date_range(
        start=datetime.strptime(
            kwargs.pop("date_debut_prelevement"), "%Y-%m-%d"
        ).date(),
        end=datetime.strptime(
            kwargs.pop("date_fin_prelevement"), "%Y-%m-%d"
        ).date(),
    )
    dates = pd.Series(ranges).to_frame("date")
    dates["year"] = dates["date"].dt.year
    dates = dates.groupby("year")["date"].agg(["min", "max"])
    for d in "min", "max":
        dates[d] = dates[d].dt.strftime("%Y-%m-%d")
    if start_auto_determination:
        dates = pd.concat(
            [
                dates,
                pd.DataFrame([{"min": "1900-01-01", "max": "1960-01-01"}]),
            ],
            ignore_index=False,
        ).sort_index()

    dates = dates.values.tolist()

    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_environmental_conditions(
                date_debut_prelevement=date_min,
                date_fin_prelevement=date_max,
                format="geojson",
                **kwargs,
            )
            for date_min, date_max in tqdm(
                dates,
                desc="querying year/year",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


def get_analysis(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve analysis results from measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_analysis
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
            "`get_all_analysis(code_department='02')`"
        )

    # Set a loop for yearly querying as dataset are big

    start_auto_determination = False
    if "date_debut_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_debut_prelevement"] = "1960-01-01"
    if "date_fin_prelevement" not in kwargs:
        kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

    ranges = pd.date_range(
        start=datetime.strptime(
            kwargs.pop("date_debut_prelevement"), "%Y-%m-%d"
        ).date(),
        end=datetime.strptime(
            kwargs.pop("date_fin_prelevement"), "%Y-%m-%d"
        ).date(),
    )
    dates = pd.Series(ranges).to_frame("date")
    dates["year"] = dates["date"].dt.year
    dates = dates.groupby("year")["date"].agg(["min", "max"])
    for d in "min", "max":
        dates[d] = dates[d].dt.strftime("%Y-%m-%d")
    if start_auto_determination:
        dates = pd.concat(
            [
                dates,
                pd.DataFrame([{"min": "1900-01-01", "max": "1960-01-01"}]),
            ],
            ignore_index=False,
        ).sort_index()

    dates = dates.values.tolist()

    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_analysis(
                date_debut_prelevement=date_min,
                date_fin_prelevement=date_max,
                format="geojson",
                **kwargs,
            )
            for date_min, date_max in tqdm(
                dates,
                desc="querying year/year",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results
