#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convenience functions for phytopharmaceuticals transactions consumption :
retrieve all bought/sold substances/products on a given territory.

Note: geographical dataset for postal codes may be downloaded freely from
https://datanova.laposte.fr/data-fair/api/v1/datasets/laposte-hexasmal/full
"""

from datetime import date
from itertools import product
import warnings

import pandas as pd
from tqdm import tqdm

from cl_hubeau.phytopharmaceuticals_transactions.phytopharmaceuticals_scraper import (
    PhytopharmaceuticalsSession,
)
from cl_hubeau import _config
from cl_hubeau.exceptions import UnexpectedValueError

from cl_hubeau.utils import (
    get_regions,
    get_departements,
    get_departements_from_regions,
    _get_postcodes,
)


def _get_territory_years_combination(
    kwargs: dict,
    transaction: str,
    filter_regions: tuple = (),
    filter_departements: tuple = (),
) -> list:
    """
    Prepare a list of available combinations of years/territories to loop
    the queries.

    Note that kwargs is changed in place: code_territoire, annee_min, annee_max
    will be popped out of it if possible.

    Parameters
    ----------
    kwargs : dict
        kwargs passed to the API
    transaction : str
        Kind of queried transactions of phytopharmaceuticals, should either be
        "bought" or "sold"
    filter_regions : tuple, optional
        Inner argument, used to filter either departements or postcodes on a
        given subset of regions. The default is (), which results to no
        filtering.
        This argument is ignored if type_territoire is among "National",
        "Région".
    filter_departements : tuple, optional
        Inner argument, used to filter postcodes on a  given subset of
        departements. The default is (), which results to no filtering.
        This argument is ignored if type_territoire is among "National",
        "Région" or "Département".

    Raises
    ------
    UnexpectedValueError
        if transaction not among expected values
        if type_territoire not among expected values

    Returns
    -------
    combinations : list
        combinations of tuples (territory_code, year).

    Examples
    ------
    >>> _get_territory_years_combination({"type_territoire": "National"}, "bought")
    [('', 2013), ('', 2014), ... ('', 2024), ('', 2025)]

    >>> _get_territory_years_combination(
        {"type_territoire": "Région"},
        "bought"
        )
    [('01', 2013), ('01', 2014), ..., ('94', 2024), ('94', 2025)]

    >>> _get_territory_years_combination(
        {"type_territoire": "Zone postale"},
        filter_departements="59",
        transaction="bought"
        )
    [('59268', 2013), ('59268', 2014), ..., ('59272', 2024), ('59272', 2025)]

    >>> _get_territory_years_combination(
        {"type_territoire": "Département", "annee_min": 2020},
        filter_regions="32",
        transaction="bought"
        )
    [('02', 2020), ('02', 2021), ..., ('80', 2024), ('80', 2025)]

    """

    if transaction not in {"bought", "sold"}:
        raise UnexpectedValueError(
            "transaction", transaction, ["bought", "sold"]
        )

    type_territoire = kwargs.pop("type_territoire", "National")
    code_territoire = kwargs.pop("code_territoire", "")

    warn_regs = bool(filter_regions)
    warn_deps = bool(filter_departements)

    if type_territoire == "National":
        # both regs & deps filtering are ignored
        pass

    elif type_territoire == "Région":
        # both regs & deps filtering are ignored
        code_territoire = code_territoire if code_territoire else get_regions()

    elif type_territoire == "Département":
        if code_territoire:
            # already filtered directly with deps codes, ignore
            # filter_regions
            pass
        elif filter_regions:
            # deps filtering is ignored
            warn_regs = False
            code_territoire = get_departements_from_regions(filter_regions)
        else:
            code_territoire = get_departements()

    elif type_territoire == "Zone postale":
        warn_regs = False
        warn_deps = False
        code_territoire = (
            code_territoire
            if code_territoire
            else _get_postcodes(filter_regions, filter_departements).tolist()
        )
    else:
        raise UnexpectedValueError(
            "code_territoire",
            type_territoire,
            ["National", "Région", "Département", "Zone postale"],
        )
    msg = f"is ignored with type_territoire='{type_territoire}'"
    if warn_regs:
        warnings.warn(f"`filter_regions` {msg}", stacklevel=2)
    if warn_deps:
        warnings.warn(f"`filter_departements` {msg}", stacklevel=2)

    if isinstance(code_territoire, str) or not code_territoire:
        code_territoire = [code_territoire]

    annee_max = kwargs.pop("annee_max", None)

    if transaction == "bought":
        annee_min = kwargs.pop("annee_min", None) or 2013
    elif transaction == "sold":
        annee_min = kwargs.pop("annee_min", None) or 2008

    annee_max = annee_max if annee_max else date.today().year
    years = range(annee_min, annee_max + 1)

    combinations = list(product(code_territoire, years))
    return combinations


def _get_all_from_loop(
    transaction: str,
    retrieve: str,
    filter_regions: tuple = (),
    filter_departements: tuple = (),
    **kwargs,
) -> pd.DataFrame:
    """
    Inner function to retrieve data using an inner loop on years and
    territories.

    Parameters
    ----------
    transaction : str
        Either "bought" or "sold".
    retrieve : str
        Either "substance" or "product"
    filter_regions : tuple, optional
        Used to restrict a query to a given region, when type_territoire is
        either 'Département' or 'Zone postal'.
        This will be ignored for type_territoire = 'National' or even
        'Région' (in that case, you should filter the dataset using the
        official API argument `code_territoire`)
    filter_departements : tuple, optional
        Used to restrict a query to a given region, when type_territoire is
        'Zone postal'.
        This will be ignored for type_territoire = 'National' or 'Région' or
        even 'Département' (in that case, you should filter the dataset using
        the official API argument `code_territoire`)
    **kwargs :
        kwargs passed to PhytopharmaceuticalsSession (hence exclusively
        intended for hub'eau API's arguments).

    Raises
    ------
    ValueError
        If transaction or retrieve do not match expected values.

    Returns
    -------
    results : pd.DataFrame
        DataFrame of bought/sold substances/products

    """

    type_territoire = kwargs.get("type_territoire", "National")

    combinations_territory_year = _get_territory_years_combination(
        kwargs=kwargs,
        transaction=transaction,
        filter_regions=filter_regions,
        filter_departements=filter_departements,
    )

    desc = (
        "querying "
        + ("territory/territory & " if type_territoire != "National" else "")
        + "year/year"
    )

    func = transaction, retrieve
    if func == ("bought", "substance"):
        func = "active_substances_bought"
    elif func == ("bought", "product"):
        func = "phytopharmaceutical_products_bought"
    elif func == ("sold", "substance"):
        func = "active_substances_sold"
    elif func == ("sold", "product"):
        func = "phytopharmaceutical_products_sold"
    else:
        raise ValueError(f"found {func}")

    with PhytopharmaceuticalsSession() as session:
        results = [
            getattr(session, func)(
                type_territoire=type_territoire,
                code_territoire=(
                    id_territory if id_territory != [None] else ""
                ),
                annee_min=year,
                annee_max=year,
                **kwargs,
            )
            for id_territory, year in tqdm(
                combinations_territory_year,
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


def get_all_active_substances_bought(
    filter_regions: tuple = (), filter_departements: tuple = (), **kwargs
) -> pd.DataFrame:
    """
    Retrieve all active substances bought on a given territory.

    An inner loop is used to avoid reaching the 20k threshold (loop over
    territories and years).

    Note the following differences from raw Hub'Eau endpoint :
    * you can use either a `filter_regions` or `filter_departements` argument
      to query the results on a given region/departement


    Parameters
    ----------
    filter_regions : tuple, optional
        Inner argument, used to filter either departements or postcodes on a
        given subset of regions. The default is (), which results to no
        filtering.
        This argument is ignored if type_territoire is among "National",
        "Région".
    filter_departements : tuple, optional
        Inner argument, used to filter postcodes on a  given subset of
        departements. The default is (), which results to no filtering.
        This argument is ignored if type_territoire is among "National",
        "Région" or "Département".
    **kwargs :
        Any of the argument accepted by the API's endpoint. See
        PhytopharmaceuticalsSession.active_substances_bought's docstring.

    Returns
    -------
    pd.DataFrame
        Collected dataset.

    Examples
    -------
    >>> df = get_all_active_substances_bought()

    >>> df = get_all_active_substances_bought(
        type_territoire="Région", code_territoire="32"
    )

    >>> df = get_all_active_substances_bought(
        type_territoire="Zone postale", filter_departements=["59", "62"]
    )

    """

    return _get_all_from_loop(
        transaction="bought",
        retrieve="substance",
        filter_regions=filter_regions,
        filter_departements=filter_departements,
        **kwargs,
    )


def get_all_phytopharmaceutical_products_bought(
    filter_regions=(), filter_departements=(), **kwargs
):
    """
    Retrieve all phytopharmaceutical products bought on a given territory.

    An inner loop is used to avoid reaching the 20k threshold (loop over
    territories and years).

    Note the following differences from raw Hub'Eau endpoint :
    * you can use either a `filter_regions` or `filter_departements` argument
      to query the results on a given region/departement

    Parameters
    ----------
    filter_regions : tuple, optional
        Inner argument, used to filter either departements or postcodes on a
        given subset of regions. The default is (), which results to no
        filtering.
        This argument is ignored if type_territoire is among "National",
        "Région".
    filter_departements : tuple, optional
        Inner argument, used to filter postcodes on a  given subset of
        departements. The default is (), which results to no filtering.
        This argument is ignored if type_territoire is among "National",
        "Région" or "Département".
    **kwargs :
        Any of the argument accepted by the API's endpoint. See
        PhytopharmaceuticalsSession.phytopharmaceutical_products_bought's
        docstring.

    Returns
    -------
    pd.DataFrame
        Collected dataset.

    Examples
    -------
    >>> df = get_all_phytopharmaceutical_products_bought()

    >>> df = get_all_phytopharmaceutical_products_bought(
        type_territoire="Région", code_territoire="32"
    )

    >>> df = get_all_phytopharmaceutical_products_bought(
        type_territoire="Zone postale", filter_departements=["59", "62"]
    )

    """

    return _get_all_from_loop(
        transaction="bought",
        retrieve="product",
        filter_regions=filter_regions,
        filter_departements=filter_departements,
        **kwargs,
    )


def get_all_active_substances_sold(
    filter_regions=(), filter_departements=(), **kwargs
):
    """
    Retrieve all active substances sold on a given territory.

    An inner loop is used to avoid reaching the 20k threshold (loop over
    territories and years).

    Note the following differences from raw Hub'Eau endpoint :
    * you can use either a `filter_regions` or `filter_departements` argument
      to query the results on a given region/departement

    Parameters
    ----------
    filter_regions : tuple, optional
        Inner argument, used to filter either departements or postcodes on a
        given subset of regions. The default is (), which results to no
        filtering.
        This argument is ignored if type_territoire is among "National",
        "Région".
    filter_departements : tuple, optional
        Inner argument, used to filter postcodes on a  given subset of
        departements. The default is (), which results to no filtering.
        This argument is ignored if type_territoire is among "National",
        "Région" or "Département".
    **kwargs :
        Any of the argument accepted by the API's endpoint. See
        PhytopharmaceuticalsSession.active_substances_sold's docstring.

    Returns
    -------
    pd.DataFrame
        Collected dataset.

    Examples
    -------
    >>> df = get_all_active_substances_sold()

    >>> df = get_all_active_substances_sold(
        type_territoire="Région", code_territoire="32"
    )

    >>> df = get_all_active_substances_sold(
        type_territoire="Département", filter_regions=["32", "11"]
    )

    """
    return _get_all_from_loop(
        transaction="sold",
        retrieve="substance",
        filter_regions=filter_regions,
        filter_departements=filter_departements,
        **kwargs,
    )


def get_all_phytopharmaceutical_products_sold(
    filter_regions=(), filter_departements=(), **kwargs
):
    """
    Retrieve all phytopharmaceutical products sold on a given territory.

    An inner loop is used to avoid reaching the 20k threshold (loop over
    territories and years).

    Note the following differences from raw Hub'Eau endpoint :
    * you can use either a `filter_regions` or `filter_departements` argument
      to query the results on a given region/departement.

    Parameters
    ----------
    filter_regions : tuple, optional
        Inner argument, used to filter either departements or postcodes on a
        given subset of regions. The default is (), which results to no
        filtering.
        This argument is ignored if type_territoire is among "National",
        "Région".
    filter_departements : tuple, optional
        Inner argument, used to filter postcodes on a  given subset of
        departements. The default is (), which results to no filtering.
        This argument is ignored if type_territoire is among "National",
        "Région" or "Département".
    **kwargs :
        Any of the argument accepted by the API's endpoint. See
        PhytopharmaceuticalsSession.phytopharmaceutical_products_bought's
        docstring.

    Returns
    -------
    pd.DataFrame
        Collected dataset.

    Examples
    -------
    >>> df = get_all_phytopharmaceutical_products_sold()

    >>> df = get_all_phytopharmaceutical_products_sold(
        type_territoire="Région", code_territoire="32"
    )

    >>> df = get_all_phytopharmaceutical_products_sold(
        type_territoire="Département", filter_regions=["32", "11"]
    )

    """

    return _get_all_from_loop(
        transaction="sold",
        retrieve="product",
        filter_regions=filter_regions,
        filter_departements=filter_departements,
        **kwargs,
    )
