#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convenience functions for hydrobiology consumption
"""

from functools import partial
from typing import Union
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm

from cl_hubeau.hydrobiology.hydrobiology_scraper import HydrobiologySession
from cl_hubeau import _config
from cl_hubeau.utils.fill_missing_fields import (
    _fill_missing_cog,
    _fill_missing_basin_subbasin,
)
import cl_hubeau.utils.mesh
from cl_hubeau.utils import _prepare_kwargs

PROPAGATION_OK = {
    "bbox",
    "code_bassin",
    "code_commune",
    "code_cours_eau",
    "code_departement",
    "code_masse_eau",
    "code_region",
    "code_sous_bassin",
    "code_station_hydrobio",
    "codes_reseaux",
    "distance",
    "latitude",
    "longitude",
    "libelle_station_hydrobio",
}


tqdm_partial = partial(
    tqdm,
    leave=_config["TQDM_LEAVE"],
    position=tqdm._get_free_pos(),
)


def get_all_stations(
    fill_values: bool = True, **kwargs
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Retrieve all stations from France.

    Use a loop to avoid reaching 20k results threshold.

    Parameters
    ----------
    fill_values :
        if True, will try to consolidate data (french official geographic code,
        basin and subbasins). Default is True
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

    if "bbox" in kwargs:
        # bbox is set, use it directly and hope for the best
        bbox = kwargs.pop("bbox", "")
        if isinstance(bbox, str):
            bbox = bbox.split(",")
    elif not any(
        kwargs.get(x) for x in areas_from_fixed_mesh | areas_without_mesh
    ):
        # no specific location -> let's set a default mesh to avoid reaching
        # the 20k threshold
        bbox = cl_hubeau.utils.mesh._get_mesh(side=5)
    elif any(kwargs.get(x) for x in areas_from_fixed_mesh):
        # a key has been given for which cl-hubeau fixes the queries, using a
        # custom mesh/bbox
        area_dict = {
            k: v for k, v in kwargs.items() if k in areas_from_fixed_mesh
        }
        for k in areas_from_fixed_mesh:
            kwargs.pop(k, None)
        bbox = cl_hubeau.utils.mesh._get_mesh(**area_dict, side=5)
    else:
        # using keys from areas_without_mesh which are not covered by _get_mesh
        # so let's use built-in hub'eau queries
        bbox = [""]

    if "format" in kwargs and kwargs["format"] != "geojson":
        warnings.warn(
            "get_all_stations forces `format='geojson'` in order to perform "
            "data consolidation with some geodatasets"
        )
    kwargs["format"] = "geojson"

    if "fields" in kwargs:
        if isinstance(kwargs["fields"], str):
            kwargs["fields"] = kwargs["fields"].split(",")

        try:
            for area, val in area_dict.items():
                if val:
                    kwargs["fields"].append(area)
        except UnboundLocalError:
            pass

    with HydrobiologySession() as session:
        if bbox != [""]:
            results = [
                session.get_stations(bbox=this_bbox, **kwargs)
                for this_bbox in tqdm(
                    bbox,
                    desc="querying stations",
                    leave=_config["TQDM_LEAVE"],
                    position=tqdm._get_free_pos(),
                )
            ]
        else:
            results = [session.get_stations(**kwargs)]

    if not results:
        return gpd.GeoDataFrame()

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()

    results = gpd.pd.concat(results, ignore_index=True)
    results = results.drop_duplicates("code_station_hydrobio")

    if fill_values:
        results = _fill_missing_cog(
            results,
            code_commune="code_commune",
            code_departement="code_departement",
            code_region="code_region",
            libelle_commune="libelle_commune",
            libelle_departement="libelle_departement",
            libelle_region="libelle_region",
        )

        # Note : on some areas, those columns are totally empty and not
        # returned
        try:
            results["code_bassin"]
        except KeyError:
            results = results.assign(
                code_bassin=np.nan,
                code_sous_bassin=np.nan,
                libelle_sous_bassin=np.nan,
                libelle_bassin=np.nan,
            )

        results = _fill_missing_basin_subbasin(
            results,
            code_sous_bassin="code_sous_bassin",
            libelle_sous_bassin="libelle_sous_bassin",
            code_bassin="code_bassin",
            libelle_bassin="libelle_bassin",
        )

    # filter from mesh
    try:
        query = " & ".join(
            f"({k}=='{v}')" if isinstance(v, str) else f"{k}.isin({v})"
            for k, v in area_dict.items()
            if v
        )
        results = results.query(query)
    except UnboundLocalError:
        pass

    return results


def get_all_indexes(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all indexes from France.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_debut_prelevement/date_fin_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrobiologySession.get_indexes (intended for hub'eau
        API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of indexes. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    if not kwargs:
        warnings.warn(
            "get_all_indexes should only be used with "
            "kwargs, for instance `get_all_indexes(code_departement='02')`"
        )

    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=100,
        months=12,
        date_start_label="date_debut_prelevement",
        date_end_label="date_fin_prelevement",
        start_date="1970-01-01",
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_station_hydrobio",
        get_entities_func=get_all_stations,
    )

    desc = "querying year/year & 100 stations/ 100 stations"
    with HydrobiologySession() as session:
        results = [
            session.get_indexes(**kwargs, **kw_loop)
            for kw_loop in tqdm_partial(
                kwargs_loop,
                desc=desc,
            )
        ]

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    try:
        results = gpd.pd.concat(results, ignore_index=True)
    except ValueError:
        # results is empty
        return gpd.GeoDataFrame()

    return results


def get_all_taxa(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all taxa from France.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_debut_prelevement/date_fin_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to HydrobiologySession.get_taxa (intended for hub'eau
        API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of taxa. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    if not kwargs:
        warnings.warn(
            "get_all_taxa should only be used with "
            "kwargs, for instance `get_all_taxa(code_departement='02')`"
        )

    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=100,
        months=12,
        date_start_label="date_debut_prelevement",
        date_end_label="date_fin_prelevement",
        start_date="1970-01-01",
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_station_hydrobio",
        get_entities_func=get_all_stations,
    )

    desc = "querying year/year & 100 stations/ 100 stations"
    with HydrobiologySession() as session:
        results = [
            session.get_taxa(**kwargs, **kw_loop)
            for kw_loop in tqdm_partial(
                kwargs_loop,
                desc=desc,
            )
        ]

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    try:
        results = gpd.pd.concat(results, ignore_index=True)
    except ValueError:
        # results is empty
        return gpd.GeoDataFrame()

    return results
