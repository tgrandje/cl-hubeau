#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for rivers' temperature analysis
"""

import warnings

import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm


from cl_hubeau.temperature import TemperatureSession
from cl_hubeau import _config
import cl_hubeau.utils.mesh
from cl_hubeau.utils import _prepare_kwargs
from cl_hubeau.utils.fill_missing_fields import (
    _fill_missing_cog,
    _fill_missing_basin_subbasin,
)

PROPAGATION_OK = {
    "bbox",
    "code_banque_reference",
    "code_bassin",
    "code_commune",
    "code_cours_eau",
    "code_departement",
    "code_eu_masse_eau",
    "code_masse_eau",
    "code_region",
    "code_sous_bassin",
    "code_station",
    "code_troncon_hydro",
    "distance",
    "latitude",
    "libelle_bassin",
    "libelle_commune",
    "libelle_cours_eau",
    "libelle_departement",
    "libelle_masse_eau",
    "libelle_region",
    "libelle_sous_bassin",
    "libelle_station",
    "longitude",
    "type_entite_hydro",
}


def get_all_stations(fill_values: bool = True, **kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations for monitoring rivers' temperatures

    Use a loop to avoid reaching 20k results threshold.

    Parameters
    ----------
    **fill_values :
        if True, will try to consolidate data (french official geographic code,
        basin and subbasins). Default is True
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_stations
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of stations

    """

    areas_from_fixed_mesh = {
        "code_region",
        "code_departement",
        "code_commune",
        "code_bassin",
        "code_sous_bassin",
    }
    areas_without_mesh = {
        "code_eu_masse_eau",
        "code_masse_eau",
        "code_cours_eau",
        "code_station",
        "code_troncon_hydro",
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
        bbox = cl_hubeau.utils.mesh._get_mesh(side=1.5)
    elif any(kwargs.get(x) for x in areas_from_fixed_mesh):
        # a key has been given for which cl-hubeau fixes the queries, using a
        # custom mesh/bbox
        area_dict = {
            k: v for k, v in kwargs.items() if k in areas_from_fixed_mesh
        }
        for k in areas_from_fixed_mesh:
            kwargs.pop(k, None)

        bbox = cl_hubeau.utils.mesh._get_mesh(**area_dict, side=1.5)
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

    with TemperatureSession() as session:
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
    results = results.drop_duplicates("code_station")

    if fill_values:
        results = _fill_missing_cog(results)

        # Note : on some areas, those columns might be totally empty and not
        # returned
        try:
            results["code_bassin"]
        except KeyError:
            results = results.assign(
                code_bassin=np.nan,
                code_sous_bassin=np.nan,
                libelle_bassin=np.nan,
                libelle_sous_bassin=np.nan,
            )

        results = _fill_missing_basin_subbasin(results)

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


def get_all_chronicles(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve chronicles for temperature measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_debut_mesure/date_fin_mesure are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to TemperatureSession.get_chronicles
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of operations

    """

    if not kwargs:
        warnings.warn(
            "get_all_chronicles should only be used with "
            "kwargs, for instance `get_all_chronicles(code_departement='02')`"
        )

    kwargs, kwargs_loop = _prepare_kwargs(
        kwargs,
        chunks=200,
        months=12,
        date_start_label="date_debut_mesure",
        date_end_label="date_fin_mesure",
        start_date="2005-01-01",
        propagation_safe=PROPAGATION_OK,
        code_entity_primary_key="code_station",
        get_entities_func=get_all_stations,
    )

    desc = "querying year/year & 200 stations/ 200 stations"
    with TemperatureSession() as session:

        results = [
            session.get_chronicles(
                **kwargs,
                **kw_loop,
            )
            for kw_loop in tqdm(
                kwargs_loop,
                desc=desc,
                leave=_config["TQDM_LEAVE"],
                position=tqdm._get_free_pos(),
            )
        ]
    results = [x.dropna(axis=1, how="all") for x in results if not x.empty]

    if not results:
        return pd.DataFrame()

    results = pd.concat(results, ignore_index=True)
    return results


if __name__ == "__main__":
    df = get_all_chronicles(code_departement="974", format="geojson")
