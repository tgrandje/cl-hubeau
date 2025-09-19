#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for fish API
"""

from functools import partial
from typing import Union
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm


from cl_hubeau.fish import (
    FishSession,
)
from cl_hubeau import _config
import cl_hubeau.utils.mesh
from cl_hubeau.utils.fill_missing_fields import (
    _fill_missing_cog,
    _fill_missing_basin_subbasin,
)
from cl_hubeau.utils import _prepare_kwargs

PROPAGATION_OK = {
    "bbox",
    "code_bassin",
    "code_commune",
    "code_departement",
    "code_entite_hydrographique",
    "code_point_prelevement",
    "code_point_prelevement_aspe",
    "code_region",
    "code_station",
    "latitude",
    "libelle_bassin",
    "libelle_commune",
    "libelle_departement",
    "libelle_entite_hydrographique",
    "libelle_region",
    "libelle_station",
    "longitude",
    "distance",
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
    Retrieve all stations for analyses on fish

    Use a loop to avoid reaching 20k results threshold.

    Parameters
    ----------
    fill_values :
        if True, will try to consolidate data (french official geographic code,
        basin and subbasins). Default is True
    **kwargs :
        kwargs passed to FishSession.get_stations
        (hence mostly intended for hub'eau API's arguments).
        Note that you can also query the dataset specifying "code_sous_bassin"
        as this is handled by cl-hubeau natively (even if this is not a hub'eau
        argument).

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
        "code_entite_hydrographique",
        "code_masse_eau",
        "code_point_prelevement",
        "code_point_prelevement_aspe",
        "code_station",
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

    with FishSession() as session:
        if bbox != [""]:
            results = [
                session.get_stations(bbox=this_bbox, **kwargs)
                for this_bbox in tqdm_partial(
                    bbox,
                    desc="querying stations",
                )
            ]
        else:
            results = [session.get_stations(**kwargs)]

    if not results:
        return gpd.GeoDataFrame()

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return gpd.GeoDataFrame()
    results = gpd.pd.concat(results, ignore_index=True)

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

        # missing sub-basins on API's return, we can fill those.
        results = results.assign(
            code_sous_bassin=np.nan, libelle_sous_bassin=np.nan
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

    try:
        results = results.drop_duplicates("code_point_prelevement_aspe")
    except KeyError:
        pass

    return results


def get_all_observations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve observations

    Should be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_operation_min/date_operation_max are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to FishSession.get_observations
        (hence mostly intended for hub'eau API's arguments).
        Note that you can also query the dataset specifying "code_sous_bassin"
        as this is handled by cl-hubeau natively (even if this is not a hub'eau
        argument).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of analysis results

    """

    if not kwargs:
        warnings.warn(
            "get_all_observations should only be used with "
            "kwargs, for instance "
            "`get_all_observations(code_departement='02')`"
        )

    chunks = 200
    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=chunks,
        months=12,
        date_start_label="date_operation_min",
        date_end_label="date_operation_max",
        start_date="1965-01-01",
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_point_prelevement_aspe",
        get_entities_func=get_all_stations,
    )

    desc = f"querying year/year & {chunks} stations/ {chunks} stations"
    with FishSession() as session:
        results = [
            session.get_observations(**kwargs, **kw_loop)
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


def get_all_operations(**kwargs) -> pd.DataFrame:
    """
    Retrieve operations from multiple stations.

    Parameters
    ----------
    codes_entites : list of stations
        List of site
    **kwargs :
        kwargs passed to FishSession.get_stations
        (hence mostly intended for hub'eau API's arguments).
        Note that you can also query the dataset specifying "code_sous_bassin"
        as this is handled by cl-hubeau natively (even if this is not a hub'eau
        argument).

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    chunks = 200
    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=chunks,
        months=120,
        date_start_label="date_operation_min",
        date_end_label="date_operation_max",
        start_date="1960-01-01",
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_point_prelevement_aspe",
        get_entities_func=get_all_stations,
    )

    desc = f"querying 10 year / 10 year & {chunks} stations/ {chunks} stations"
    with FishSession() as session:
        results = [
            session.get_operations(**kwargs, **kw_loop)
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


def get_all_indicators(**kwargs) -> pd.DataFrame:
    """
    Retrieve indicators from multiple stations.

    Parameters
    ----------
    codes_entites : list of stations
        List of site
    **kwargs :
        kwargs passed to FishSession.get_stations
        (hence mostly intended for hub'eau API's arguments).
        Note that you can also query the dataset specifying "code_sous_bassin"
        as this is handled by cl-hubeau natively (even if this is not a hub'eau
        argument).

    Returns
    -------
    results : pd.dataFrame
        DataFrame of results

    """

    chunks = 200
    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=chunks,
        months=120,
        date_start_label="date_operation_min",
        date_end_label="date_operation_max",
        start_date="1960-01-01",
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_point_prelevement_aspe",
        get_entities_func=get_all_stations,
    )

    desc = f"querying 10 year / 10 year & {chunks} stations/ {chunks} stations"
    with FishSession() as session:
        results = [
            session.get_indicators(**kwargs, **kw_loop)
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
