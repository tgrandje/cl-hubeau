#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for water abstraction
"""

import geopandas as gpd
import pandas as pd
from functools import partial
from tqdm import tqdm
import warnings
from typing import Union
from cl_hubeau import _config
import cl_hubeau.utils.mesh

from cl_hubeau.water_abstraction import AbstractionSession

tqdm_partial = partial(
    tqdm,
    leave=_config["TQDM_LEAVE"],
    position=tqdm._get_free_pos(),
)


def get_all_ouvrages(
    fill_values: bool = True, **kwargs
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Retrieve all ouvrages for points on water abstraction

    Use a loop to avoid reaching 20k results threshold.

    Parameters
    ----------
    fill_values :
        if True, will try to consolidate data (french official geographic code.
        Default is True
    **kwargs :
        kwargs passed to AbstractionSession.get_ouvrages
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of stations. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    areas_from_fixed_mesh = {
        "code_departement",
        "code_commune_insee",
    }
    areas_without_mesh = {
        "code_bdlisa",
        "code_entite_hydro_cours_eau",
        "code_entite_hydro_plan_eau",
        "code_mer_ocean",
        "code_ouvrage",
        "code_type_milieu",
        "codes_points_prelevements",
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

    with AbstractionSession() as session:
        if bbox != [""]:
            results = [
                session.get_ouvrages(bbox=this_bbox, **kwargs)
                for this_bbox in tqdm_partial(
                    bbox,
                    desc="querying ouvrages",
                )
            ]
        else:
            results = [session.get_ouvrages(**kwargs)]

    if not results:
        return gpd.GeoDataFrame()

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return gpd.GeoDataFrame()
    results = gpd.pd.concat(results, ignore_index=True)

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
        results = results.drop_duplicates("codes_points_prelevements")
    except KeyError:
        pass

    return results


def get_all_points_prelevement(
    fill_values: bool = True, **kwargs
) -> gpd.GeoDataFrame:
    """
    Retrieve all abstraction points for monitoring water abstraction

    Use a loop to avoid reaching 20k results threshold.
    """

    if "fields" in kwargs:
        if isinstance(kwargs["fields"], str):
            kwargs["fields"] = kwargs["fields"].split(",")

    with AbstractionSession() as session:
        results = [session.get_points_prelevement(**kwargs)]
    if not results:
        return gpd.GeoDataFrame()

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return pd.DataFrame()
    results = gpd.pd.concat(results, ignore_index=True)
    results = results.drop_duplicates("code_point_prelevement")

    return results


def get_all_chronicles(
    fill_values: bool = True, **kwargs
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Retrieve all chronicles for water abstraction

    Use a loop to avoid reaching 20k results threshold.

    Parameters
    ----------
    fill_values :
        if True, will try to consolidate data (french official geographic code.
        Default is True
    **kwargs :
        kwargs passed to AbstractionSession.get_ouvrages
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : Union[gpd.GeoDataFrame, pd.DataFrame]
        (Geo)DataFrame of stations. The result will be of type DataFrame only
        if `format="json"` has been specifically set.

    """

    areas_from_fixed_mesh = {
        "code_departement",
        "code_commune_insee",
    }
    areas_without_mesh = {
        "code_ouvrage",
        "producteur_donnee",
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

    with AbstractionSession() as session:
        if bbox != [""]:
            results = [
                session.get_ouvrages(bbox=this_bbox, **kwargs)
                for this_bbox in tqdm_partial(
                    bbox,
                    desc="querying ouvrages",
                )
            ]
        else:
            results = [session.get_ouvrages(**kwargs)]

    if not results:
        return gpd.GeoDataFrame()

    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]
    if not results:
        return gpd.GeoDataFrame()
    results = gpd.pd.concat(results, ignore_index=True)

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
        results = results.drop_duplicates("codes_points_prelevements")
    except KeyError:
        pass

    return results


# if __name__ == "__main__":
#     df = get_all_points_prelevement(code_departement='59')
#     print(df)
