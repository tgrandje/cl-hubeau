#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilitary functions to prepare temporal and territorial loops
"""

from datetime import datetime, timedelta, date
from itertools import product

import pandas as pd


def prepare_kwargs_loops(
    key_start: str,
    key_end: str,
    kwargs: dict,
    start_auto_determination: bool,
    months: float = 6,
) -> dict:
    """
    Prepare a list of kwargs of arguments to prepare a temporal loop.

    WARNING : kwargs is changed by side-effect!

    Parameters
    ----------
    key_start : str
        Field representing the start of a timestep in the API (for instance,
        "date_debut_prelevement")
    key_end : str
        Field representing the end of a timestep in the API (for instance,
        "date_fin_prelevement")
    kwargs : dict
        kwargs passed to a higher level function.
    start_auto_determination : bool
        Whether the dates were automatically set by the algorithm.
    months : float
        Number of consecutive months to split the data into

    Returns
    -------
    args : dict
        List of dict (kwargs) for inner loops. It should have 2 or 3 keys:
            * key_start
            * key_end
            * code_departement (optional)

    """
    start = datetime.strptime(kwargs.pop(key_start), "%Y-%m-%d").date()
    end = datetime.strptime(kwargs.pop(key_end), "%Y-%m-%d").date()

    if months == 0.5:
        freq = "SME"
    elif months == 0.25:
        freq = "W"
    elif months % 12 == 0:
        freq = f"{months//12}YS"
    else:
        freq = f"{months}MS"
    ranges = pd.date_range(start, end=end, freq=freq)
    dates = pd.Series(ranges).to_frame("min")
    dates["max"] = dates["min"].shift(-1) - timedelta(days=1)
    dates.at[dates.index.max(), "max"] = pd.Timestamp(end)

    for d in "min", "max":
        dates[d] = dates[d].dt.strftime("%Y-%m-%d")
    dates = dates.reset_index(drop=True)

    if start_auto_determination:
        dates = pd.concat(
            [
                dates,
                pd.DataFrame(
                    [
                        {
                            "min": "1900-01-01",
                            "max": (
                                datetime.strptime(
                                    dates["min"].min(), "%Y-%m-%d"
                                )
                                - timedelta(days=1)
                            ).strftime("%Y-%m-%d"),
                        }
                    ]
                ),
            ],
            ignore_index=True,
        ).sort_values("min")
    else:
        dates.at[0, "min"] = start.strftime("%Y-%m-%d")
        dates.at[dates.index.max(), "max"] = end.strftime("%Y-%m-%d")
    args = dates.rename(
        {"min": key_start, "max": key_end},
        axis=1,
    )

    for x in (
        "code_region",
        "code_departement",
        "code_commune",
        # groundwater quality endpoint:
        "num_departement",
        "code_insee_actuel",
    ):
        if x in kwargs:
            territory = kwargs.pop(x)
            if isinstance(territory, str):
                territory = [territory]
            args = args.merge(pd.Series(territory, name=x), how="cross")

    # Force restitution of new results at first hand, to trigger
    # ValueError >20k results faster
    args = args.sort_values(key_end, ascending=False)

    args = args.to_dict(orient="records")

    return args


def _prepare_kwargs(
    kwargs: dict,
    chunks: int,
    months: int,
    date_start_label: str,
    date_end_label: str,
    start_date: str,
    propagation_safe: list,
    code_entity_primary_key: str,
    get_entities_func: callable,
) -> tuple[dict, list[dict]]:
    """
    Prepare kwargs & kwargs_loop to run temporal loops to acquire all results.

    This allow to gather all stations corresponding to given kwargs and then
    prepare the kwargs / kwargs_loop to run a double loop on stations and
    6 months subsets.

    Parameters
    ----------
    kwargs :
        initial kwargs passed to the upper-level function (hence should
        correspond to the API's available arguments). Note that kwargs is a
        dict and NOT a **kwargs, as some arguments will be popped IN PLACE.
    chunks : int
        Number of entities to query simultaneously. For instance 50. This might
        be restricted by the API's endpoint, most often to 200 entities.
    months : int
        Number of months to query simultaneously in the dateranges. For
        instance 12 months to run queries for yearly subsets.
    date_start_label : str
        API argument used to reference the start of a timelaps, for instance
        "date_debut_prelevement". This is used to create timeranges on which
        the upper-level functions should loop.
    date_end_label : str
        API argument used to reference the end of a timelaps, for instance
        "date_fin_prelevement". This is used to create timeranges on which
        the upper-level functions should loop.
    start_date : str
        First date used to start the timeranges. Should be of the %Y-%m-%d
        format, for instance "1960-01-01". This should obviously be older than
        the oldest data in the queried dataset.
    propagation_safe : list
        List of argument which are safe to propagate from the desired endpoint
        to the "framework" (ie most often "station") to restrict the loop.
        For instance, it is safe to propagate `code_station='eggs'` or
        `code_cours_eau='spam'` but it is **NOT** safe to propagate
        `code_parametre='bacon'` or `date_debut_prelevement='sausage'`. This
        list should be unique for all endpoints of a given API served by
        Hub'Eau.
    code_entity_primary_key : str
        Primary key of the "framework" entity (ie most often "station"), as
        a column label in the "framework" output from Hub'Eau. For instance,
        "code_station".
    get_entities_func : callable
        Function used to retrieve the "fra. For instance

    Returns
    -------
    kwargs : dict
        Fixed kwargs to pass to the API low-level function
    kwargs_loop : list[dict]
        kwargs to pass to the API low-level function, varying at each iteration
        of the loop get_all_stations.

    """
    start_auto_determination = False
    if date_start_label not in kwargs:
        start_auto_determination = True
        kwargs[date_start_label] = start_date
    if date_end_label not in kwargs:
        kwargs[date_end_label] = date.today().strftime("%Y-%m-%d")

    # safe arguments to propagate to the station endpoint:
    copy = {k: v for k, v in kwargs.items() if k in propagation_safe}
    [kwargs.pop(x) for x in copy]
    copy.update({"fields": code_entity_primary_key, "format": "geojson"})
    stations = get_entities_func(**copy, fill_values=False)
    stations = stations[code_entity_primary_key].values.tolist()
    stations = [
        stations[i : i + chunks]  # noqa
        for i in range(0, len(stations), chunks)
    ]

    kwargs["format"] = kwargs.get("format", "geojson")

    kwargs_loop = prepare_kwargs_loops(
        date_start_label,
        date_end_label,
        kwargs,
        start_auto_determination,
        months=months,
    )

    kwargs_loop = list(product(stations, kwargs_loop))
    kwargs_loop = [
        {**{code_entity_primary_key: chunk}, **kw} for chunk, kw in kwargs_loop
    ]
    kwargs_loop = sorted(
        kwargs_loop, key=lambda x: x[date_start_label], reverse=True
    )

    return kwargs, kwargs_loop
