#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for hydrometry consumption
"""

from datetime import date, datetime
from itertools import product

import pandas as pd
from tqdm import tqdm


from cl_hubeau.drinking_water_quality import DrinkingWaterQualitySession
from cl_hubeau import _config
from cl_hubeau.utils import get_cities


def get_all_water_networks(**kwargs) -> pd.DataFrame:
    """
    Retrieve all UDI from France.

    Use a loop to avoid reaching 20k results threshold. Do not use
    `code_commune` as they are set by the current function.

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

    city_codes = get_cities().tolist()

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

        results = pd.concat(results, ignore_index=True)
    return results


def get_control_results(
    codes_reseaux: list = None, codes_communes: list = None, **kwargs
) -> pd.DataFrame:
    """
    Retrieve sanitary controls' results.

    Uses a loop to avoid reaching 20k results threshold.
    As queries may induce big datasets, loops are based on networks and years,
    even if date_min_prelevement/date_max_prelevement are not set.

    Note that `codes_reseaux` and `codes_communes` are mutually exclusive!

    Parameters
    ----------
    codes_reseaux : list, optional
        List of networks to retrieve data from. The default is None.
    codes_communes : list, optional
        List of city codes to retrieve data from. The default is None.
    **kwargs :
        kwargs passed to DrinkingWaterQualitySession.get_control_results
        (hence mostly intended for hub'eau API's arguments). Do not use
        `code_reseau` or `code_commune` as they are set by the current
        function.

    Returns
    -------
    results : pd.DataFrame
        DataFrame of sanitary control results.

    """

    if codes_reseaux and codes_communes:
        raise ValueError(
            "only one argument allowed among codes_reseaux and codes_communes"
        )
    if not codes_reseaux and not codes_communes:
        raise ValueError(
            "exactly one argument must be set among codes_reseaux and codes_communes"
        )

    # Split by 20-something chunks
    codes_names = "code_commune" if codes_communes else "code_reseau"
    codes = codes_communes if codes_communes else codes_reseaux
    codes = [codes[i : i + 20] for i in range(0, len(codes), 20)]

    # Set a loop for yearly querying as dataset are big
    start_auto_determination = False
    if "date_min_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_min_prelevement"] = "2016-01-01"
    if "date_max_prelevement" not in kwargs:
        kwargs["date_max_prelevement"] = date.today().strftime("%Y-%m-%d")

    ranges = pd.date_range(
        start=datetime.strptime(
            kwargs.pop("date_min_prelevement"), "%Y-%m-%d"
        ).date(),
        end=datetime.strptime(
            kwargs.pop("date_max_prelevement"), "%Y-%m-%d"
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
                pd.DataFrame([{"min": "1900-01-01", "max": "2015-12-31"}]),
            ],
            ignore_index=False,
        ).sort_index()

    args = list(product(codes, dates.values.tolist()))

    with DrinkingWaterQualitySession() as session:

        results = [
            session.get_control_results(
                date_min_prelevement=date_min,
                date_max_prelevement=date_max,
                **{codes_names: chunk},
                **kwargs
            )
            for chunk, (date_min, date_max) in tqdm(
                args,
                desc="querying network/network and year/year",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    results = pd.concat(results, ignore_index=True)
    return results


if __name__ == "__main__":
    df = get_control_results(
        codes_communes="59350",
        code_parametre="1340",
        date_min_prelevement="2023-01-01",
        date_max_prelevement="2023-12-31",
    )
    print(df)
