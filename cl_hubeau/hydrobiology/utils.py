#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for piezometry consumption
"""

from copy import deepcopy

# from datetime import date
# from itertools import product

from typing import Union

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from cl_hubeau.hydrobiology.hydrobiology_scraper import HydrobiologySession
from cl_hubeau import _config

from cl_hubeau.utils.mesh import _get_mesh
from cl_hubeau.utils.fill_missing_fields import (
    _fill_missing_cog,
    _fill_missing_basin_subbasin,
)

# from cl_hubeau.utils import prepare_kwargs_loops


def get_all_stations(**kwargs) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Retrieve all stations from France.

    If no location is specified (neither geographic nor hydrographic locator)
    a loop will be introduced on each region.

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrobiologySession.get_stations (intended for hub'eau
        API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of stations. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    areas_from_fixed_mesh = {
        "code_region",
        "code_departement",
        "code_commune",
        "code_bassin",
        "code_sous_bassin",
    }
    areas_without_mesh = {
        "code_masse_eau",
        "code_cours_eau",
        "code_station_hydrobio",
    }
    if not any(
        kwargs.get(x) for x in areas_from_fixed_mesh | areas_without_mesh
    ):
        # no specific location -> let's loop over regions to avoid reaching
        # the 20k threshold
        bbox = _get_mesh(side=1.5)
        kept = None
    elif any(kwargs.get(x) for x in areas_from_fixed_mesh):
        # a key has been given for which cl-hubeau fixes the queries, using a
        # custom mesh/bbox
        bbox = _get_mesh(**kwargs)
        kept = {x: kwargs.pop(x, None) for x in areas_from_fixed_mesh}
    else:
        # using keys from areas_without_mesh which are not covered by _get_mesh
        # so let's built-in hub'eau queries
        bbox = kwargs.pop("bbox", "")
        if isinstance(bbox, str):
            bbox = bbox.split(",")
        kept = None

    kwargs["format"] = kwargs.get("format", "geojson")

    with HydrobiologySession() as session:
        if bbox != [""]:
            results = [
                session.get_stations(bbox=this_bbox, **kwargs)
                for this_bbox in tqdm(
                    bbox,
                    desc="querying",
                    leave=_config["TQDM_LEAVE"],
                    position=tqdm._get_free_pos(),
                )
            ]
        else:
            results = [session.get_stations(**kwargs)]

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    try:
        results = gpd.pd.concat(results, ignore_index=True)
    except ValueError:
        # results is empty
        return gpd.GeoDataFrame()

    results = _fill_missing_cog(
        results,
        code_commune="code_commune",
        code_departement="code_departement",
        code_region="code_region",
        libelle_commune="libelle_commune",
        libelle_departement="libelle_departement",
        libelle_region="libelle_region",
    )
    results = _fill_missing_basin_subbasin(
        results,
        code_sous_bassin="code_sous_bassin",
        libelle_sous_bassin="libelle_sous_bassin",
        code_bassin="code_bassin",
        libelle_bassin="libelle_bassin",
    )

    # filter from mesh
    if kept:
        query = " & ".join(f"({k}=='{v}')" for k, v in kept.items() if v)
        results = results.query(query)

    try:
        results = results.drop_duplicates("code_station_hydrobio")
    except KeyError:
        pass

    return results


def get_all_indexes(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all indexes from France.

    If no location is specified (neither geographic nor hydrographic locator)
    a loop will be introduced on each departement.

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrobiologySession.get_indexes (intended for hub'eau
        API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of stations. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    # areas_from_fixed_mesh = {
    #     "code_region",
    #     "code_departement",
    #     "code_commune",
    #     "code_bassin",
    #     "code_sous_bassin",
    # }
    # areas_without_mesh = {
    #     "code_masse_eau",
    #     "code_cours_eau",
    #     "code_station_hydrobio",
    # }
    # if not any(
    #     kwargs.get(x) for x in areas_from_fixed_mesh | areas_without_mesh
    # ):
    #     # no specific location -> let's loop over regions to avoid reaching
    #     # the 20k threshold
    #     bbox = _get_mesh(side=0.5)
    # elif any(kwargs.get(x) for x in areas_from_fixed_mesh):
    #     # a key has been given for which cl-hubeau fixes the queries, using a
    #     # custom mesh/bbox
    #     bbox = _get_mesh(**kwargs)
    #     kept = {x: kwargs.pop(x, None) for x in areas_from_fixed_mesh}
    # else:
    #     # using keys from areas_without_mesh which are not covered by _get_mesh
    #     # so let's built-in hub'eau queries
    #     bbox = kwargs.pop("bbox", "")
    #     if isinstance(bbox, str):
    #         bbox = bbox.split(",")

    # elif kwargs.get(""):
    #     # TODO : query on FRG_ALA gets 410519 results!
    #     pass
    # else:
    #     code_region = kwargs.pop("code_region")
    #     if isinstance(code_region, str):
    #         code_region = code_region.split(",")

    # TODO : date_debut_prelevement

    kwargs["format"] = kwargs.pop("format", "geojson")

    copy = deepcopy(kwargs)
    copy["fields"] = "code_station_hydrobio"

    stations = get_all_stations(**kwargs)
    stations = stations["code_station_hydrobio"].values.tolist()

    with HydrobiologySession() as session:
        results = [
            session.get_indexes(code_station_hydrobio=station, **kwargs)
            for station in tqdm(
                stations,
                desc="querying",
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]

    # # Set a loop for yearly querying as dataset are big
    # start_auto_determination = False
    # if "date_debut_prelevement" not in kwargs:
    #     start_auto_determination = True
    #     kwargs["date_debut_prelevement"] = "1971-01-01"
    # if "date_fin_prelevement" not in kwargs:
    #     kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

    # timeranges = prepare_kwargs_loops(
    #     "date_debut_prelevement",
    #     "date_fin_prelevement",
    #     kwargs,
    #     start_auto_determination,
    #     split_months=12,
    # )

    # kwargs_loop = [
    #     timerange | {"bbox": this_bbox}
    #     for timerange, this_bbox in product(timeranges, bbox)
    # ]

    # with HydrobiologySession() as session:
    #     results = [
    #         session.get_indexes(**kwargs, **kw)
    #         for kw in tqdm(
    #             kwargs_loop,
    #             desc="querying",
    #             leave=_config["TQDM_LEAVE"],
    #             position=tqdm._get_free_pos(),
    #         )
    #     ]

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    try:
        results = gpd.pd.concat(results, ignore_index=True)
    except ValueError:
        # results is empty
        return gpd.GeoDataFrame()

    results = _fill_missing_cog(
        results,
        code_commune="code_commune",
        code_departement="code_departement",
        code_region="code_region",
        libelle_commune="libelle_commune",
        libelle_departement="libelle_departement",
        libelle_region="libelle_region",
    )
    results = _fill_missing_basin_subbasin(
        results,
        code_sous_bassin="code_sous_bassin",
        libelle_sous_bassin="libelle_sous_bassin",
        code_bassin="code_bassin",
    )

    # filter from mesh
    # if kept:
    #     query = " & ".join(f"({k}=='{v}')" for k, v in kept.items() if v)
    #     results = results.query(query)

    try:
        results = results.drop_duplicates("code_station_hydrobio")
    except KeyError:
        pass

    return results


def get_all_taxa(**kwargs) -> gpd.GeoDataFrame:
    # TODO
    pass


if __name__ == "__main__":
    df = get_all_stations()
    # df = get_all_indexes()
    # df.plot()
