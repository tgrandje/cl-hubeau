# -*- coding: utf-8 -*-
"""
Base Hub'eau session (derived from requests-cache's Session) to use across
all APIs.
"""

from copy import deepcopy
from datetime import datetime
import logging
import os
from typing import Callable
from urllib.parse import urlparse, parse_qs
import warnings

import geopandas as gpd
import pandas as pd
import pebble
from requests import Session
from requests.exceptions import JSONDecodeError
from requests_cache import CacheMixin
from requests_ratelimiter import LimiterMixin
from tqdm import tqdm

from cl_hubeau.constants import DIR_CACHE, CACHE_NAME
from cl_hubeau import _config


def map_func(
    threads: int,
    func: Callable,
    iterables: list,
    disable: bool = False,
) -> list:
    """
    Map a function against an iterable of arguments.

    Map an API call, looping over pages, with a tqdm progressbar to track the
    progress. This is meant to be used when the API's "cursor" is a simple
    integer which can be computed beforehand. In case of hashed cursor returned
    by the API, use map_func_recursive instead.

    If threads > 1, this will use multithreading. If threads == 1, a simple
    iteration over the arguments will be done.

    Parameters
    ----------
    threads : int
        Number of allowed threads.
        If threads==1 will deactivate multithreading: use this for debugging.
    func : Callable
        Function do map
    iterables : list
        Collection of arguments for func
    disable : bool, optional
        Set to True do disable the tqdm progressbar. The default is False

    Returns
    -------
    results : list
        Collection of results

    """

    total = len(iterables)
    results = []
    with tqdm(
        desc="querying",
        total=total,
        leave=_config["TQDM_LEAVE"],
        disable=disable,
        position=tqdm._get_free_pos(),
    ) as pbar:

        if threads > 1:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    ".*Connection pool is full, discarding connection.*",
                )
                with pebble.ThreadPool(threads) as pool:
                    future = pool.map(func, iterables)
                    iterator = future.result()
                    while True:
                        try:
                            results += next(iterator)
                        except StopIteration:
                            break

                        pbar.update()
        else:
            for x in iterables:
                results += func(x)
                pbar.update()

    return results


class BaseHubeauSession(CacheMixin, LimiterMixin, Session):
    """
    Base session class to use across cl_hubeau for querying APIs from Hub'Eau
    """

    BASE_URL = "https://hubeau.eaufrance.fr/api"
    CACHE_NAME = os.path.join(DIR_CACHE, CACHE_NAME)
    ALLOWABLE_CODES = [200, 206, 400]

    def __init__(
        self,
        expire_after: int = _config["DEFAULT_EXPIRE_AFTER"],
        proxies: dict = None,
        size: int = _config["SIZE"],
        per_second: int = _config["RATE_LIMITER"],
        version: str = None,
        **kwargs,
    ):
        """
        Initialize a CachedSession object with optional proxies and a
        ratelimiter of 10/sec. (cache excluded)

        Parameters
        ----------
        expire_after : int, optional
            Default expiration timeout. The default is DEFAULT_EXPIRE_AFTER
            from config file.
        proxies : dict, optional
            Optional corporate proxies for internet connection.
            The default is None.
            For instance {"https": 'http://my-awesome-proxy:8888'}
            If not set, will be infered from os environment variables
            http_proxy and https_proxy (if set)
        size : int, optional
            Size set for each page. Default is SIZE from config file.
        per_second : int, optional
            Max authorized rate of requests per second. Default is RATE_LIMITER
            from config file.
        version : str, optional
            API's version. If set and not coherent with the current API's
            version returned by hubeau, will trigger a warning.
        **kwargs
            Optional kwargs passed to the CachedSession class constructor.

        Returns
        -------
        None.

        """

        self.size = size
        self.version = version

        super().__init__(
            cache_name=self.CACHE_NAME,
            expire_after=expire_after,
            allowable_codes=self.ALLOWABLE_CODES,
            per_second=per_second,
            **kwargs,
        )
        if proxies:
            self.proxies.update(proxies)
        else:
            self.proxies.update(
                {
                    "https": os.environ.get("https_proxy", None),
                    "http": os.environ.get("http_proxy", None),
                }
            )

    @staticmethod
    def list_to_str_param(
        x: list,
        max_authorized_values: int = None,
        exact_authorized_values: int = None,
    ) -> str:
        """
        Join array of arguments to an accepted string format

        Parameters
        ----------
        x : list
            List of arguments
        max_authorized_values : int, optional
            Maximum authorized values in the list
        exact_authorized_values : int, optional
            Exact authorized values in the list

        Returns
        -------
        str
            Concatenated arguments

        """
        if any(isinstance(x, y) for y in (list, tuple, set)):
            if max_authorized_values and len(x) > max_authorized_values:
                msg = (
                    f"Should not have more than {max_authorized_values}, "
                    f"found {len(x)} instead"
                )
                raise ValueError(msg)
            if exact_authorized_values and len(x) != exact_authorized_values:
                msg = (
                    f"Should have exactly {exact_authorized_values}, "
                    f"found {len(x)} instead"
                )
                raise ValueError(msg)

            return ",".join(x)
        if isinstance(x, str):
            return x
        raise ValueError(f"unexpected format for {x}")

    @staticmethod
    def ensure_date_format_is_ok(date_str: str) -> None:
        """
        Raise an exception if date is not in an accepted format

        Parameters
        ----------
        date_str : str
            Date argument (as a string)

        Raises
        ------
        ValueError
            In the date's format is forbidden

        Returns
        -------
        None

        """
        try:
            datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                "hubeau date should respect yyyy-MM-dd format"
            ) from exc

    def request(
        self,
        method: str,
        url: str,
        *args,
        **kwargs,
    ):
        logging.info(f"{method=} {url=} {args=} {kwargs=}")
        r = super().request(
            method,
            url,
            *args,
            **kwargs,
        )
        if not r.ok:
            try:
                error = r.json()
            except JSONDecodeError:
                error = str(r.content)
            raise ValueError(
                f"Connection error on {method=} {url=} with {kwargs=}, "
                f"got {error}"
            )
        return r

    def get_result(
        self, method: str, url: str, params: dict, **kwargs
    ) -> pd.DataFrame:
        """
        Loop over API's results until last page is reached and aggregate
        results.

        Parameters
        ----------
        method : str
            http method ("GET" or "POST" mostly)
        url : str
            url to query
        params : dict
            params to add to request
        **kwargs :
            other arguments are passed to CachedSession.request

        Raises
        ------
        ValueError
            When results length does not match the number of expected results.

        Returns
        -------
        pd.DataFrame
            API's results, which will be of (gpd.GeoDataFrame if format is
            a geojson)

        """

        copy_params = deepcopy(params)
        copy_params["size"] = 1
        # copy_params["page"] = 1
        js = self.request(
            method=method, url=url, params=copy_params, **kwargs
        ).json()

        if self.version:
            try:
                if not self.version == js["api_version"]:
                    warnings.warn(
                        "This API's version is not consistent with the "
                        "expected one from cl_hubeau package: "
                        "unexpected behaviour may occur."
                    )
            except KeyError:
                logging.warning("api_version not found among API response")

        logging.debug(js)

        page = "page" if "page" in js["first"] else "cursor"

        count_rows = js["count"]
        msg = f"{count_rows} expected results"
        logging.info(msg)
        count_pages = count_rows // self.size + (
            0 if count_rows % self.size == 0 else 1
        )

        params["size"] = self.size
        iterables = [deepcopy(params) for x in range(count_pages)]
        for x in range(count_pages):
            iterables[x].update({page: x + 1})

        # Multithreading only if page - mono-thread if cursor instead
        threads = min(_config["THREADS"], count_pages) if page == "page" else 1

        key = (
            "data"
            if not ("format" in params and params["format"] == "geojson")
            else "features"
        )

        if page == "page":

            def func(params):
                """
                Simple query to gather one result, every params are known
                before consumming the API.
                """
                r = self.request("GET", url=url, params=params, **kwargs)
                r = r.json()[key]
                return r

        else:

            def func(params):
                """
                Recursive query to gather all results, each next url being
                unkown beforehand: each query should be resolved to know the
                next cursor value. Update the progressbar at each yield
                """
                r = self.request("GET", url=url, params=params, **kwargs)
                result = r.json()[key]

                try:
                    next_url = r.json()["next"]
                    cursor = parse_qs(urlparse(next_url).query)["cursor"][0]
                except KeyError:
                    yield result
                pbar.update()
                try:
                    new_params = deepcopy(params)
                    new_params["cursor"] = cursor
                    yield from func(new_params)
                    yield result
                except UnboundLocalError:
                    pass

        # Deactivate progress bar if less pages than available threads
        disable = count_pages <= threads

        if page == "page":
            # if integer cursor ("page" param), use multithreading to gather
            # data faster
            results = map_func(threads, func, iterables, disable)
        else:
            # if hashed cursor ("cursor" param), use recursive function to
            # gather all results
            with tqdm(
                desc="querying",
                total=count_pages,
                leave=_config["TQDM_LEAVE"],
                disable=disable,
                position=tqdm._get_free_pos(),
            ) as pbar:
                results = [y for x in func(params) for y in x]

        if "format" in params and params["format"] == "geojson":
            if results:
                results = gpd.GeoDataFrame.from_features(results, crs=4326)
            else:
                results = gpd.GeoDataFrame()
        else:
            if results:
                results = pd.DataFrame(results)
            else:
                results = pd.DataFrame()
        if not len(results) == count_rows:
            # Note that for "big" realtime datasets, the result's length may
            # differ at the end from what was expected at the start of
            # iterations
            msg = (
                f"results do not match expected results for {url} {params} - "
                f"expected {count_rows}, got {len(results)} instead"
            )
            warnings.warn(msg)
        return results
