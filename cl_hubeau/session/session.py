# -*- coding: utf-8 -*-
"""
Base Hub'eau session (derived from requests-cache's Session) to use across
all APIs.
"""

from copy import deepcopy
from datetime import datetime
from functools import lru_cache
import hashlib
import logging
import os
import socket
from typing import Callable, Any, Union
from urllib.parse import urlparse, parse_qs
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import pebble
from pyrate_limiter import SQLiteBucket
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import JSONDecodeError
from requests_cache import CacheMixin
from requests_ratelimiter import LimiterMixin
from urllib3.util.retry import Retry

from cl_hubeau.constants import DIR_CACHE, CACHE_NAME, RATELIMITER_NAME
from cl_hubeau import _config, __version__
from cl_hubeau.exceptions import UnexpectedValueError

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def log_only_once(url):
    logger.warning("api_version not found among API response")


def map_func(
    threads: int,
    func: Callable,
    iterables: list,
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

    Returns
    -------
    results : list
        Collection of results

    """

    results = []

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

    else:
        for x in iterables:
            results += func(x)

    return results


class BaseHubeauSession(CacheMixin, LimiterMixin, Session):
    """
    Base session class to use across cl_hubeau for querying APIs from Hub'Eau
    """

    BASE_URL = "https://hubeau.eaufrance.fr/api"
    CACHE_NAME = os.path.join(DIR_CACHE, CACHE_NAME)
    RATELIMITER_PATH = os.path.join(DIR_CACHE, RATELIMITER_NAME)
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

        for key in ["bucket_class", "bucket_kwargs"]:
            try:
                del kwargs[key]
            except KeyError:
                pass
        bucket_class = SQLiteBucket
        bucket_kwargs = {
            "path": self.RATELIMITER_PATH,
            "isolation_level": "EXCLUSIVE",
            "check_same_thread": False,
        }

        super().__init__(
            cache_name=self.CACHE_NAME,
            expire_after=expire_after,
            allowable_codes=self.ALLOWABLE_CODES,
            per_second=per_second,
            bucket_class=bucket_class,
            bucket_kwargs=bucket_kwargs,
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

        retry = Retry(
            10, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("http://", adapter)
        self.mount("https://", adapter)

        self.headers.update({"User-Agent": self.get_machine_user_agent()})

    @staticmethod
    def get_machine_user_agent() -> str:
        """
        Get a fixed User Agent for a given machine.

        Returns
        -------
        str
            User-Agent string
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]

        m = hashlib.sha256()
        m.update(bytes(ip, encoding="utf8"))
        digest = m.hexdigest()

        return f"cl_hubeau-{__version__}-{digest}"

    @staticmethod
    def list_to_str_param(
        x: list,
        max_authorized_values: int = None,
        exact_authorized_values: int = None,
        authorized_values: Union[list, set, tuple] = None,
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
        authorized_values : Union[list, set, tuple], optional
            If set, each individual value from x should be among
            authorized_values. Default is None.

        Returns
        -------
        str
            Concatenated arguments

        """
        if isinstance(x, str):
            x = [y.strip() for y in x.split(",")]
        if isinstance(x, (list, tuple, set)):
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
            if authorized_values:
                authorized_values = {str(y) for y in authorized_values}
                violation = [str(y) for y in x if y not in authorized_values]
                if violation:
                    raise ValueError(
                        f"unauthorized values found for {x} : {violation}"
                    )

            return ",".join([str(y) for y in x])
        raise ValueError(f"unexpected format found on {x}")

    @staticmethod
    def _ensure_val_among_authorized_values(
        arg: str, kwargs: dict, allowed: Any, converter: Callable = None
    ) -> Any:
        """
        Pops an argument from kwargs and check that it matches

        Parameters
        ----------
        arg : str
            key of argument to pop from kwargs
        kwargs : dict
            kwargs to pop argument from
        allowed : Any
            Allowed values
        converter : Callable
            Function to be run on kwargs[arg] if found (for instance, `int`)

        Raises
        ------
        UnexpectedValueError
            If the value is not allowed.

        Returns
        -------
        variable : Any
            Poped value from kwargs

        """
        variable = kwargs.pop(arg)
        if converter:
            variable = converter(variable)
        if variable not in allowed:
            raise UnexpectedValueError(arg, variable, allowed)
        return variable

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
                "cl-hubeau date should respect yyyy-MM-dd format"
            ) from exc

    def request(
        self,
        method: str,
        url: str,
        *args,
        **kwargs,
    ):
        logger.info(
            "method=%s url=%s args=%s kwargs=%s", method, url, args, kwargs
        )
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
                f"got {error} : got result {r.status_code}"
            )
        return r

    def get_result(
        self,
        method: str,
        url: str,
        params: dict,
        time_start: str = None,
        time_end: str = None,
        **kwargs,
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
        time_start : str, optional
            Can be set in order to auto-adjust the temporal loop when > 20k
            results have been found. In that case, time_start must take the
            value of Hub'Eau's argument on the start of the timeserie
            (for instance, "date_debut_prelevement"). The default is None which
            will deactivate this option.
        time_end : str, optional
            Can be set in order to auto-adjust the temporal loop when > 20k
            results have been found. In that case, time_end must take the
            value of Hub'Eau's argument on the end of the timeserie
            (for instance, "date_fin_prelevement"). The default is None which
            will deactivate this option.
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

        remove = [key for key, val in params.items() if val == ""]
        for key in remove:
            del params[key]

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
                log_only_once(url)

        logger.debug(js)

        page = "page" if "page" in js["first"] else "cursor"

        count_rows = js["count"]
        if count_rows > 20_000:
            if not (time_start and time_end):
                raise ValueError(
                    "this request won't be handled by hubeau "
                    f"( {count_rows} > 20k results) - query was {params}"
                )

            timeranges = pd.date_range(
                start=params[time_start], end=params[time_end], freq="D"
            )
            timeranges = np.array_split(timeranges, 2)
            results = []
            for window in timeranges:
                params.update(
                    {
                        time_start: window.min().strftime("%Y-%m-%d"),
                        time_end: window.max().strftime("%Y-%m-%d"),
                    }
                )
                results.append(
                    self.get_result(
                        method,
                        url,
                        params,
                        time_start=time_start,
                        time_end=time_end,
                        **kwargs,
                    )
                )
                results = [
                    x.dropna(axis=1, how="all") for x in results if not x.empty
                ]
                if not results:
                    return pd.DataFrame()
            return pd.concat(results)

        msg = f"{count_rows} expected results"
        logger.info(msg)
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
                try:
                    new_params = deepcopy(params)
                    new_params["cursor"] = cursor
                    yield from func(new_params)
                    yield result
                except UnboundLocalError:
                    pass

        if page == "page":
            # if integer cursor ("page" param), use multithreading to gather
            # data faster
            results = map_func(threads, func, iterables)
        else:
            # if hashed cursor ("cursor" param), use recursive function to
            # gather all results
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
