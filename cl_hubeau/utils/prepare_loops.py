#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilitary functions to prepare temporal and territorial loops
"""
import pandas as pd
from datetime import datetime


def prepare_kwargs_loops(kwargs: dict, start_auto_determination: bool) -> dict:
    """
    Prepare a list of kwargs of arguments to prepare a temporal loop.

    WARNING : kwargs is changed by side-effect!

    Parameters
    ----------
    kwargs : dict
        kwargs passed to a higher level function.
    start_auto_determination : bool
        Whether the dates were automatically set by the algorithm.

    Returns
    -------
    args : dict
        List of dict (kwargs) for inner loops. It should have 2 or 3 keys:
            * date_debut_prelevement
            * date_fin_prelevement
            * code_departement (optional)

    """
    start = datetime.strptime(
        kwargs.pop("date_debut_prelevement"), "%Y-%m-%d"
    ).date()
    end = datetime.strptime(
        kwargs.pop("date_fin_prelevement"), "%Y-%m-%d"
    ).date()
    ranges = pd.date_range(start, end=end, freq="6ME")
    dates = pd.Series(ranges).to_frame("max")
    dates["min"] = dates["max"].shift(1)
    dates = dates.dropna().loc[:, ["min", "max"]]
    for d in "min", "max":
        dates[d] = dates[d].dt.strftime("%Y-%m-%d")
    dates = dates.reset_index(drop=True)

    if start_auto_determination:
        dates = pd.concat(
            [
                dates,
                pd.DataFrame([{"min": "1900-01-01", "max": "1960-01-01"}]),
            ],
            ignore_index=False,
        ).sort_index()
    else:
        dates.at[0, "min"] = start.strftime("%Y-%m-%d")
        dates.at[dates.index.max(), "max"] = end.strftime("%Y-%m-%d")
    args = dates.rename(
        {"min": "date_debut_prelevement", "max": "date_fin_prelevement"},
        axis=1,
    )

    if "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if isinstance(deps, str):
            deps = [deps]
        args = args.merge(
            pd.Series(deps, name="code_departement"), how="cross"
        )

    # Force restitution of new results at first hand, to trigger
    # ValueError >20k results faster
    args = args.sort_values("date_fin_prelevement", ascending=False)

    args = args.to_dict(orient="records")

    return args
