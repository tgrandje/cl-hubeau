#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for superficial waterbodies quality inspections
"""

from datetime import date
from itertools import product
import warnings

from deprecated import deprecated
import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm


from cl_hubeau.superficial_waterbodies_quality import (
    SuperficialWaterbodiesQualitySession,
)
from cl_hubeau import _config
import cl_hubeau.utils.mesh
from cl_hubeau.utils import prepare_kwargs_loops
from cl_hubeau.utils.fill_missing_fields import (
    _fill_missing_cog,
    _fill_missing_basin_subbasin,
)

PROPAGATION_OK = {
    "bbox",
    "code_banque_reference",
    "code_bassin_dce",
    "code_commune",
    "code_cours_eau",
    "code_departement",
    "code_eu_masse_eau",
    "code_masse_eau",
    "code_region",
    "code_reseau",
    "code_sous_bassin",
    "code_station",
    "distance",
    "latitude",
    "libelle_commune",
    "libelle_departement",
    "libelle_masse_eau",
    "libelle_region",
    "libelle_reseau",
    "libelle_station",
    "longitude",
    "nom_bassin_dce",
    "nom_cours_eau",
    "nom_sous_bassin",
    "type_entite_hydro",
}


def get_all_stations(fill_values: bool = True, **kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve all stations for physical/chemical analyses on superficial
    waterbodies

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
        DataFrame of networks (UDI) /cities coverage

    """

    areas_from_fixed_mesh = {
        "code_region",
        "code_departement",
        "code_commune",
        "code_bassin_dce",
        "code_sous_bassin",
    }
    areas_without_mesh = {"code_masse_eau", "code_cours_eau", "code_station"}
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

        # quick and dirty hack as this endpoint uses code_bassin_dce instead
        # of code_bassin which is _get_mesh's kwarg
        area_dict["code_bassin"] = area_dict.pop("code_bassin_dce", None)

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

    with SuperficialWaterbodiesQualitySession() as session:
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

        # Note 1: code_bassin label will change according to the output format
        # https://github.com/BRGM/hubeau/issues/246
        # Note 2 : on some areas, those columns are totally empty and not
        # returned
        try:
            results["codeBassinDce"]
        except KeyError:
            results = results.assign(
                codeBassinDce=np.nan,
                code_eu_sous_bassin=np.nan,
                nom_sous_bassin=np.nan,
                nom_bassin=np.nan,
            )

        results = _fill_missing_basin_subbasin(
            results,
            code_sous_bassin="code_eu_sous_bassin",
            libelle_sous_bassin="nom_sous_bassin",
            code_bassin="codeBassinDce",
            libelle_bassin="nom_bassin",
        )

    # filter from mesh
    try:
        area_dict["codeBassinDce"] = area_dict.pop("code_bassin", None)
        area_dict["code_eu_sous_bassin"] = area_dict.pop(
            "code_sous_bassin", None
        )
        query = " & ".join(
            f"({k}=='{v}')" if isinstance(v, str) else f"{k}.isin({v})"
            for k, v in area_dict.items()
            if v
        )
        results = results.query(query)
    except UnboundLocalError:
        pass

    return results


def _prepare_kwargs(kwargs) -> tuple[dict, list[dict]]:
    """
    Prepare kwargs & kwargs_loop to run temporal loops to acquire all results.

    This allow to gather all stations corresponding to given kwargs and then
    prepare the kwargs / kwargs_loop to run a double loop on stations and
    6 months subsets.

    Parameters
    ----------
    kwargs : TYPE
        initial kwargs passed to the upper-level function (hence should
        correspond to the API's available arguments)

    Returns
    -------
    kwargs : dict
        Fixed kwargs to pass to the API low-level function
    kwargs_loop : list[dict]
        kwargs to pass to the API low-level function, varying at each iteration
        of the loop

    """
    start_auto_determination = False
    if "date_debut_prelevement" not in kwargs:
        start_auto_determination = True
        kwargs["date_debut_prelevement"] = "1960-01-01"
    if "date_fin_prelevement" not in kwargs:
        kwargs["date_fin_prelevement"] = date.today().strftime("%Y-%m-%d")

    # safe arguments to propagate to the station endpoint:
    propagation_ok = PROPAGATION_OK
    copy = {k: v for k, v in kwargs.items() if k in propagation_ok}
    [kwargs.pop(x) for x in copy]
    copy.update({"fields": "code_station", "format": "geojson"})
    stations = get_all_stations(**copy, fill_values=False)
    stations = stations["code_station"].values.tolist()
    stations = [
        stations[i : i + 50] for i in range(0, len(stations), 50)  # noqa
    ]

    kwargs["format"] = kwargs.get("format", "geojson")

    kwargs_loop = prepare_kwargs_loops(
        "date_debut_prelevement",
        "date_fin_prelevement",
        kwargs,
        start_auto_determination,
        months=6,
    )

    kwargs_loop = list(product(stations, kwargs_loop))
    kwargs_loop = [
        {**{"code_station": chunk}, **kw} for chunk, kw in kwargs_loop
    ]

    return kwargs, kwargs_loop


def get_all_operations(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve operations for measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_operations
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of operations

    """

    if not kwargs:
        warnings.warn(
            "get_all_operations should only be used with "
            "kwargs, for instance `get_operations(code_departement='02')`"
        )

    kwargs, kwargs_loop = _prepare_kwargs(kwargs)

    desc = "querying 6m/6m & station/station"
    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_operations(
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


def get_all_environmental_conditions(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve environmental conditions for measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_environmental_conditions
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of environmental conditions

    """

    if not kwargs:
        warnings.warn(
            "get_all_environmental_conditions should only be used with "
            "kwargs, for instance "
            "`get_all_environmental_conditions(code_department='02')`"
        )

    kwargs, kwargs_loop = _prepare_kwargs(kwargs)

    desc = "querying 6m/6m & station/station"
    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_environmental_conditions(**kwargs, **kw_loop)
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


@deprecated(version="0.6.0", reason="Please use `get_all_analyses` instead.")
def get_all_analysis(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve analyses results from measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on yearly subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_analyses
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of analyses results

    """
    return get_all_analyses(**kwargs)


def get_all_analyses(**kwargs) -> gpd.GeoDataFrame:
    """
    Retrieve analyses results from measures.

    Should only be used with additional arguments to avoid reaching the 20k
    threshold, in conjonction with the built-in loop (which will operate
    on 6 months subsets, even if date_min_prelevement/date_max_prelevement are
    not set.)

    Parameters
    ----------
    **kwargs :
        kwargs passed to SuperficialWaterbodiesQualitySession.get_analyses
        (hence mostly intended for hub'eau API's arguments).

    Returns
    -------
    results : gpd.GeoDataFrame
        GeoDataFrame of analyses results

    """

    if not kwargs:
        warnings.warn(
            "get_all_analyses should only be used with "
            "kwargs, for instance "
            "`get_all_analyses(code_department='02')`"
        )

    kwargs, kwargs_loop = _prepare_kwargs(kwargs)

    desc = "querying 6m/6m & station/station"
    with SuperficialWaterbodiesQualitySession() as session:

        results = [
            session.get_analyses(**kwargs, **kw_loop)
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
