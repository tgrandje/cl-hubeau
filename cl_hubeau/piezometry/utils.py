#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for piezometry consumption
"""

import geopandas as gpd
import pandas as pd
from tqdm import tqdm
import warnings

from cl_hubeau.piezometry.piezometry_scraper import PiezometrySession
from cl_hubeau import _config
from cl_hubeau.utils import get_departements, get_departements_from_regions


def get_all_stations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all piezometers from France.

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

    if "code_region" in kwargs:
        code_region = kwargs.pop("code_region")
        deps = get_departements_from_regions(code_region)
    elif "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if not isinstance(deps, (list, set, tuple)):
            deps = [deps]
    elif any(x in kwargs for x in ("code_commune", "code_bss", "bss_id")):
        deps = [""]
    else:
        deps = get_departements()

    with PiezometrySession() as session:

        kwargs["format"] = kwargs.get("format", "geojson")

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
        results["code_bss"]
        results = results.drop_duplicates("code_bss")
    except KeyError:
        pass
    return results


def _get_codes_bss(kwargs) -> tuple[str, list]:
    """
    Retrieve the available `code_bss` for a given territory

    Note: kwargs is changed **IN PLACE** by side-effect, this is not a
    double-starred-kwargs !

    Return : label, list_of_codes
    """

    if "code_bss" in kwargs or "bss_id" in kwargs:
        label = "code_bss" if "code_bss" in kwargs else "bss_id"
        codes = kwargs.pop(label)
        if isinstance(codes, str):
            codes = codes.split(",")

        conflicts = [
            "code_region",
            "code_departement",
            "code_commune",
        ]
        if any(x for x in conflicts if x in kwargs):
            raise ValueError(
                "only one argument allowed among either 'code_commune', "
                "'code_departement', 'code_region' in the one hand AND "
                f"'{label}' in the other hand."
            )
        if label == "code_bss" and "bss_id" in kwargs:
            raise ValueError(
                "only one argument allowed among 'code_bss' and 'bss_id' "
                "is supported"
            )

    else:

        kwargs_bss = {"format": "json"}
        if "code_region" in kwargs:
            code_region = kwargs.pop("code_region")
            deps = get_departements_from_regions(code_region)
            kwargs_bss["code_departement"] = deps
        elif "code_departement" in kwargs:
            deps = kwargs.pop("code_departement")
            kwargs_bss["code_departement"] = deps
        elif "code_commune" in kwargs:
            kwargs_bss["code_commune"] = kwargs.pop("code_commune")

        # retrieve code_entites
        label = "code_bss"
        codes_bss = get_all_stations(fields=[label], **kwargs_bss)
        codes = codes_bss["code_bss"].tolist()

    return label, codes


def get_chronicles(**kwargs) -> pd.DataFrame:
    """
    Retrieve chronicles from multiple piezometers.

    Use an inner loop for multiple piezometers to avoid reaching 20k results
    threshold from hub'eau API.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use a code_region, code_departement or code_commune argument to
      query the results on a given area

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

    if "codes_bss" in kwargs:
        msg = (
            "`codes_bss` is deprecated and will be removed in a future "
            "version, please use `code_bss` instead"
        )
        warnings.warn(msg, category=FutureWarning, stacklevel=2)
        kwargs["code_bss"] = kwargs.pop("codes_bss")

    label, codes_bss = _get_codes_bss(kwargs)
    if label != "code_bss":
        raise ValueError("code_bss is mandatory")

    with PiezometrySession() as session:
        results = [
            session.get_chronicles(code_bss=code, **kwargs)
            for code in tqdm(
                codes_bss,
                desc="querying piezo/piezo",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()
    results = pd.concat(results, ignore_index=True)
    return results


def get_realtime_chronicles(**kwargs) -> pd.DataFrame:
    """
    Retrieve realtimes chronicles from multiple piezometers.
    Uses a reduced timeout for cache expiration.

    Note that `code_bss` and `bss_id` are mutually exclusive to allow cl-hubeau
    to handle the inner loop.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use a code_region, code_departement or code_commune argument to
      query the results on a given area

    Parameters
    ----------
    **kwargs :
        kwargs passed to PiezometrySession.get_realtime_chronicles (hence
        mostly intended for hub'eau API's arguments). Do not use `code_bss` as
        they are set by the current function.

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    if "codes_bss" in kwargs:
        msg = (
            "`codes_bss` is deprecated and will be removed in a future "
            "version, please use `code_bss` instead"
        )
        warnings.warn(msg, category=FutureWarning, stacklevel=2)
        kwargs["code_bss"] = kwargs.pop("codes_bss")

    if "bss_ids" in kwargs:
        msg = (
            "`bss_ids` is deprecated and will be removed in a future "
            "version, please use `bss_id` instead"
        )
        warnings.warn(msg, category=FutureWarning, stacklevel=2)
        kwargs["bss_id"] = kwargs.pop("bss_ids")

    label, codes = _get_codes_bss(kwargs)

    with PiezometrySession(
        expire_after=_config["DEFAULT_EXPIRE_AFTER_REALTIME"]
    ) as session:
        results = [
            session.get_realtime_chronicles(**{label: code}, **kwargs)
            for code in tqdm(
                codes,
                desc="querying piezo/piezo",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()
    results = pd.concat(results, ignore_index=True)
    return results
