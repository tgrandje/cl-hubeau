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
from cl_hubeau.utils import (
    # get_departements,
    # get_departements_from_regions,
    _get_pynsee_geodata_latest,
    _get_pynsee_geolist_cities,
)

from cl_hubeau.utils.mesh import _get_mesh
from cl_hubeau.utils.hydro_perimeters_queries import _get_dce_subbasins

# from cl_hubeau.utils import prepare_kwargs_loops


def _fill_missing_cog(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Fill missing region, departement & cities elements (codes & labels) using
    the closest geometry in an buffer of 10km.

    Note: this is approximative, particularly on international borders but will
    avoid missing stations when looping over (for instance) code_regions.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame of results with missing official geographic data.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        Filled results.

    """

    if gdf[gdf.code_region.isnull()].empty:
        return gdf

    # missing region_code, departement_code, commune_code, let's fill those
    # with closest geometry, up to 10km
    crs = gdf.crs
    gdf = gdf.to_crs(2154)

    coms = _get_pynsee_geodata_latest("commune", crs=2154)
    cities_labels = _get_pynsee_geolist_cities()

    # first exact spatial join
    missing = gdf[
        (gdf.code_commune.isnull())
        | (gdf.code_departement.isnull())
        | (gdf.code_region.isnull())
    ].index
    missing = (
        gdf.loc[missing].sjoin(coms, how="left").drop("index_right", axis=1)
    )
    # get labels
    missing = missing.merge(
        cities_labels[["TITLE", "TITLE_DEP", "TITLE_REG", "CODE"]],
        how="left",
        left_on="code_commune",
        right_on="CODE",
    ).drop("CODE", axis=1)
    coalesce = {
        "code_commune": "insee_com",
        "code_departement": "insee_dep",
        "code_region": "insee_reg",
        "libelle_commune": "TITLE",
        "libelle_departement": "TITLE_DEP",
        "libelle_region": "TITLE_REG",
    }
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    # spatial join to nearest, up to 10km
    missing = gdf[
        (gdf.code_commune.isnull())
        | (gdf.code_departement.isnull())
        | (gdf.code_region.isnull())
    ].index
    missing = (
        gdf.loc[missing]
        .sjoin_nearest(
            coms,
            how="left",
            max_distance=10_000,
        )
        .drop("index_right", axis=1)
    )
    # get labels
    missing = missing.merge(
        cities_labels[["TITLE", "TITLE_DEP", "TITLE_REG", "CODE"]],
        how="left",
        left_on="code_commune",
        right_on="CODE",
    ).drop("CODE", axis=1)
    coalesce = {
        "code_commune": "insee_com",
        "code_departement": "insee_dep",
        "code_region": "insee_reg",
        "libelle_commune": "TITLE",
        "libelle_departement": "TITLE_DEP",
        "libelle_region": "TITLE_REG",
    }
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    gdf = gdf.to_crs(crs)
    return gdf


def _fill_missing_basin_subbasin(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Fill missing basin & subbasins elements (codes & labels) using
    the closest geometry in an buffer of 10km.

    Note: this is approximative, particularly on international borders but will
    avoid missing stations when looping over (for instance) basins.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame of results with missing (sub)basins data.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        Filled results.

    """

    if gdf[gdf.code_bassin.isnull()].empty:
        return gdf

    # missing code_bassin, code_sous_bassin, etc., let's fill those with
    # closest geometry, up to 10km
    crs = gdf.crs
    gdf = gdf.to_crs(2154)

    # spatial join to nearest, up to 10km
    basins = _get_dce_subbasins(crs=2154)

    missing = gdf[
        (gdf.code_sous_bassin.isnull()) | (gdf.code_bassin.isnull())
    ].index

    missing = (
        gdf.loc[missing].sjoin(basins, how="left").drop("index_right", axis=1)
    )
    coalesce = {
        "code_sous_bassin": "CdEuSsBassinDCEAdmin",
        "libelle_sous_bassin": "NomSsBassinDCEAdmin",
        "code_bassin": "CdBassinDCE",
        "libelle_bassin": "NomBassinDCE",
    }
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    missing = gdf[
        (gdf.code_sous_bassin.isnull()) | (gdf.code_bassin.isnull())
    ].index
    missing = (
        gdf.loc[missing]
        .sjoin_nearest(
            basins,
            how="left",
            max_distance=10_000,
        )
        .drop("index_right", axis=1)
    )
    coalesce = {
        "code_sous_bassin": "CdEuSsBassinDCEAdmin",
        "libelle_sous_bassin": "NomSsBassinDCEAdmin",
        "code_bassin": "CdBassinDCE",
        "libelle_bassin": "NomBassinDCE",
    }
    for key, val in coalesce.items():
        gdf[key] = gdf[key].combine_first(missing[val])

    gdf = gdf.to_crs(crs)
    return gdf


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

    kwargs["format"] = kwargs.pop("format", "geojson")

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

    results = _fill_missing_cog(results)
    results = _fill_missing_basin_subbasin(results)

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

    results = _fill_missing_cog(results)
    results = _fill_missing_basin_subbasin(results)

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
    # df = get_all_stations(code_region="32")
    df = get_all_indexes()
    df.plot()
