# -*- coding: utf-8 -*-
"""
low level class to collect data from the hydrobiology API from hub'eau
"""
import pandas as pd
from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class HydrobiologySession(BaseHubeauSession):
    """
    Base session class to handle the hydrobiology API
    """

    DOC_URL = "https://hubeau.eaufrance.fr/page/api-hydrobiologie"

    def __init__(self, *args, **kwargs):
        super().__init__(version="1.0.0", *args, **kwargs)

        # Set default size for API queries, based on hub'eau hydrobio's doc
        self.size = 5000

    def get_indexes(self, **kwargs):
        """
        Lister les indices
        Endpoint /v1/hydrobio/indices

        Ce service permet de récupérer les indices (ou résultats) biologiques.

        Doc : https://hubeau.eaufrance.fr/page/api-hydrobiologie
        """

        params = {}

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            params["sort"] = "desc"

        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            params["format"] = "json"

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in ("code_banque_reference", "code_prelevement"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in ("code_commune", "code_station_hydrobio"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 100)
            except KeyError:
                continue

        for arg in (
            "code_cours_eau",
            "code_departement",
            "code_indice",
            "code_masse_eau",
            "libelle_indice",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in ("code_bassin", "code_region", "code_sous_bassin"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 10)
            except KeyError:
                continue

        try:
            arg = "code_support"
            params[arg] = self.list_to_str_param(kwargs.pop(arg), 6)
        except KeyError:
            pass

        try:
            arg = "code_qualification"
            params[arg] = self.list_to_str_param(kwargs.pop(arg), 4)
        except KeyError:
            pass

        for arg in ("date_debut_prelevement", "date_fin_prelevement"):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "resultat_indice_max",
            "resultat_indice_min",
            "latitude",
            "longitude",
            "distance",
            "code_operation_prelevement",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/hydrobio/indices"
        df = self.get_result(
            method,
            url,
            params=params,
            time_start="date_debut_prelevement",
            time_end="date_fin_prelevement",
        )

        try:
            df["date_prelevement"] = pd.to_datetime(df["date_prelevement"])
        except KeyError:
            pass

        return df

    def get_stations(self, **kwargs):
        """
        Lister les Stations
        Endpoint /v1/hydrobio/stations_hydrobio

        Ce service permet de récupérer toutes les stations où des prélèvements
        hydrobiologiques ont eu lieu.

        Doc : https://hubeau.eaufrance.fr/page/api-hydrobiologie
        """

        params = {}

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            params["sort"] = "desc"

        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            params["format"] = "json"

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in ("code_commune", "code_station_hydrobio"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 100)
            except KeyError:
                continue

        for arg in (
            "code_cours_eau",
            "code_departement",
            "codes_indices",
            "codes_reseaux",
            "libelle_station_hydrobio",
            "code_masse_eau",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in (
            "code_bassin",
            "code_region",
            "code_sous_bassin",
            "codes_appel_taxons",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 10)
            except KeyError:
                continue

        try:
            arg = "codes_supports"
            params[arg] = self.list_to_str_param(kwargs.pop(arg), 6)
        except KeyError:
            pass

        for arg in (
            "latitude",
            "longitude",
            "distance",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            arg = "date_service"
            variable = kwargs.pop(arg)
            self.ensure_date_format_is_ok(variable)
            params[arg] = variable
        except KeyError:
            pass

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/hydrobio/stations_hydrobio"
        df = self.get_result(method, url, params=params)

        for x in "date_premier_prelevement", "date_dernier_prelevement":
            if x in df.columns:
                df[x] = pd.to_datetime(df[x])

        return df

    def get_taxa(self, **kwargs):
        """
        Lister les taxons
        Endpoint /v1/hydrobio/taxons

        Ce service permet de récupérer les listes floristiques ou faunistiques.

        Doc : https://hubeau.eaufrance.fr/page/api-hydrobiologie
        """
        params = {}

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            params["sort"] = "desc"

        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            params["format"] = "json"

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in ("code_banque_reference", "code_prelevement"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_appel_taxon",
            "code_commune",
            "code_station_hydrobio",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 100)
            except KeyError:
                continue

        for arg in (
            "code_cours_eau",
            "code_departement",
            "code_masse_eau",
            "codes_indices_operation",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in ("code_bassin", "code_region", "code_sous_bassin"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 10)
            except KeyError:
                continue

        try:
            arg = "code_support"
            params[arg] = self.list_to_str_param(kwargs.pop(arg), 6)
        except KeyError:
            pass

        try:
            arg = "code_qualification"
            params[arg] = self.list_to_str_param(kwargs.pop(arg), 4)
        except KeyError:
            pass

        for arg in ("date_debut_prelevement", "date_fin_prelevement"):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        try:
            params["code_type_resultat"] = (
                self._ensure_val_among_authorized_values(
                    "code_type_resultat", kwargs, {"1", "3", "7", "8", "9"}
                )
            )
        except KeyError:
            pass

        for arg in (
            "latitude",
            "longitude",
            "distance",
            "code_operation_prelevement",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/hydrobio/taxons"
        df = self.get_result(
            method,
            url,
            params=params,
            time_start="date_debut_prelevement",
            time_end="date_fin_prelevement",
        )

        try:
            df["date_prelevement"] = pd.to_datetime(
                df["date_prelevement"] * 1e6
            )
        except TypeError:
            # date_prelevement's format seems to change according to the args
            # see https://github.com/BRGM/hubeau/issues/247
            df["date_prelevement"] = pd.to_datetime(df["date_prelevement"])
        except KeyError:
            pass

        return df
