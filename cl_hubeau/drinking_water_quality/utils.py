#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for hydrometry consumption
"""

from datetime import date
from itertools import product
import warnings

import pandas as pd
from tqdm import tqdm


from cl_hubeau.drinking_water_quality import DrinkingWaterQualitySession
from cl_hubeau import _config
from cl_hubeau.utils import get_cities, prepare_kwargs_loops


def get_all_water_networks(**kwargs) -> pd.DataFrame:
    """
    Retrieve all UDI from France.

    Use a loop to avoid reaching 20k results threshold.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use a code_region argument to query the results on a given region

    Parameters
    ----------
    **kwargs :
        kwargs passed to DrinkingWaterQualitySession.get_cities_networks
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : pd.DataFrame
        DataFrame of networks (UDI) /cities coverage

    """

    if "code_region" in kwargs:
        city_codes = get_cities(code_region=kwargs.pop("code_region"))
    elif "code_departement" in kwargs:
        city_codes = get_cities(
            code_departement=kwargs.pop("code_departement")
        )
    elif "code_commune" in kwargs:
        city_codes = kwargs.pop("code_commune")
    else:
        city_codes = get_cities()

    # Split by 20-something chunks
    city_codes = [
        city_codes[i : i + 20] for i in range(0, len(city_codes), 20)
    ]

    with DrinkingWaterQualitySession() as session:
        results = [
            session.get_cities_networks(code_commune=chunk, **kwargs)
            for chunk in tqdm(
                city_codes,
                desc="querying city/city",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
        results = [x.dropna(axis=1, how="all") for x in results if not x.empty]

        if not results:
            return pd.DataFrame()
        results = pd.concat(results, ignore_index=True)
    return results


def get_control_results(**kwargs) -> pd.DataFrame:
    """
    Retrieve sanitary controls' results.

    Uses a loop to avoid reaching 20k results threshold.
    As queries may induce big datasets, loops are based on networks and 6 month
    timeranges, even if date_min_prelevement/date_max_prelevement are not set.

    Note the following differences from raw Hub'Eau endpoint :
    * you can use a code_region argument to query the results on a given region
    * to optimize the loops, you should only use either code_region,
      code_departement, code_commune in one hand OR code_reseau in the other
      hand. Those arguments are mutually exclusive.

    Parameters
    ----------
    **kwargs :
        kwargs passed to DrinkingWaterQualitySession.get_control_results
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : pd.DataFrame
        DataFrame of sanitary control results.

    """

    if "codes_reseaux" in kwargs:
        msg = (
            "`codes_reseaux` is deprecated and will be removed in a future "
            "version, please use `code_reseau` instead"
        )
        warnings.warn(msg, category=FutureWarning, stacklevel=2)
        kwargs["code_reseau"] = kwargs.pop("codes_reseaux")

    if "codes_communes" in kwargs:
        msg = (
            "`codes_communes` is deprecated and will be removed in a future "
            "version, please use `code_commune` instead"
        )
        warnings.warn(msg, category=FutureWarning, stacklevel=2)
        kwargs["code_commune"] = kwargs.pop("codes_communes")

    advised = [
        "code_reseau",
        "code_commune",
        "code_departement",
        "code_region",
    ]
    if not any(x in kwargs for x in advised):
        warnings.warn(
            "get_control_results should only be used with "
            "kwargs, for instance `get_control_results(code_departement='02')`"
        )

    city_codes = []
    if "code_region" in kwargs:
        city_codes = get_cities(code_region=kwargs.pop("code_region"))
    elif "code_departement" in kwargs:
        city_codes = get_cities(
            code_departement=kwargs.pop("code_departement")
        )
    elif "code_commune" in kwargs:
        city_codes = kwargs.pop("code_commune")

    if city_codes and kwargs.get("code_reseau"):
        raise ValueError(
            "only one argument allowed among either 'code_commune', "
            "'code_departement', 'code_region' in the one hand AND "
            "'code_reseau' in the other hand."
        )

    if not city_codes and not kwargs.get("code_reseau"):
        # neither code_region, code_departement, code_commune nor code_reseau
        # -> let's loop on all french cities
        city_codes = get_cities()

    # Split by 20-something chunks
    codes_names = "code_commune" if city_codes else "code_reseau"
    codes = city_codes if city_codes else kwargs.pop("code_reseau")
    codes = [codes[i : i + 20] for i in range(0, len(codes), 20)]

    # Set a loop for yearly querying as dataset are big
    start_auto_determination = False
    if "date_min_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_min_prelevement"] = "2016-01-01"
    if "date_max_prelevement" not in kwargs:
        kwargs["date_max_prelevement"] = date.today().strftime("%Y-%m-%d")

    kwargs_loop = prepare_kwargs_loops(
        "date_min_prelevement",
        "date_max_prelevement",
        kwargs,
        start_auto_determination,
    )

    kwargs_loop = list(product(codes, kwargs_loop))
    kwargs_loop = [{**{codes_names: chunk}, **kw} for chunk, kw in kwargs_loop]

    with DrinkingWaterQualitySession() as session:

        results = [
            session.get_control_results(
                **kwargs,
                **kw_loop,
            )
            for kw_loop in tqdm(
                kwargs_loop,
                desc=f"querying {codes_names}/{codes_names} and 6m/6m",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()
    results = pd.concat(results, ignore_index=True)
    return results
