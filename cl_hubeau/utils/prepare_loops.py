#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilitary functions to prepare temporal and territorial loops
"""
import pandas as pd
from datetime import datetime, timedelta


def prepare_kwargs_loops(
    key_start: str, key_end: str, kwargs: dict, start_auto_determination: bool
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
    ranges = pd.date_range(start, end=end, freq="6ME")
    dates = pd.Series(ranges).to_frame("max")
    dates["min"] = dates["max"].shift(1) + timedelta(days=1)
    dates = dates.dropna().loc[:, ["min", "max"]]
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

    if "code_departement" in kwargs:
        deps = kwargs.pop("code_departement")
        if isinstance(deps, str):
            deps = [deps]
        args = args.merge(
            pd.Series(deps, name="code_departement"), how="cross"
        )

    # Force restitution of new results at first hand, to trigger
    # ValueError >20k results faster
    args = args.sort_values(key_end, ascending=False)

    args = args.to_dict(orient="records")

    return args
