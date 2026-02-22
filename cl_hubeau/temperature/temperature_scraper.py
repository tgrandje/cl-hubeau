# -*- coding: utf-8 -*-
"""
low level class to collect data from the rivers' temperature API from hub'eau
"""

import pandas as pd

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class TemperatureSession(BaseHubeauSession):
    """
    Base session class to handle the rivers' temperatures API
    """

    DOC_URL = (
        "http://hubeau.eaufrance.fr/page/api-temperature-en-continu-cours-deau"
    )

    def __init__(self, *args, **kwargs):

        super().__init__(version="1.0.0", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 5000

    def get_stations(self, **kwargs):
        """
        Lister les stations de mesures de températures
        Endpoint /v1/temperature/station

        Ce service permet de rechercher des stations de mesures sur des cours
        d'eau et plan d'eau en France et les DROM.

        Doc: https://hubeau.eaufrance.fr/page/api-temperature-continu
        """

        params = {}

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            params["sort"] = "asc"

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

        for arg in (
            "date_debut_maj",
            "date_debut_mesure",
            "date_fin_maj",
            "date_fin_mesure",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_banque_reference",
            "code_cours_eau",
            "libelle_cours_eau",
            "code_eu_masse_eau",
            "code_masse_eau",
            "code_sous_bassin",
            "libelle_sous_bassin",
            "code_station",
            "libelle_station",
            "libelle_masse_eau",
            "type_entite_hydro",
            "code_troncon_hydro",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_bassin",
            "libelle_bassin",
            "code_region",
            "libelle_region",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 10)
            except KeyError:
                continue

        for arg in ("code_departement", "libelle_departement"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in ("code_commune", "libelle_commune"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 50)
            except KeyError:
                continue

        for arg in ("distance", "latitude", "longitude"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["exact_count"] = kwargs.pop("exact_count") in ("true", True)
        except KeyError:
            params["exact_count"] = "true"

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/temperature/station"
        df = self.get_result(method, url, params=params)

        return df

    def get_chronicles(self, **kwargs):
        """
        Lister les chroniques de température
        Endpoint /v1/temperature/chronique

        Ce service permet de rechercher des chroniques de température sur des
        cours d'eau et plan d'eau en France et les DROM.

        Doc: https://hubeau.eaufrance.fr/page/api-temperature-continu
        """

        params = {}

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            params["sort"] = "asc"

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

        for arg in (
            "date_debut_maj",
            "date_debut_mesure",
            "date_fin_maj",
            "date_fin_mesure",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_banque_reference",
            "code_cours_eau",
            "code_eu_masse_eau",
            "code_masse_eau",
            "code_qualification",
            "code_sous_bassin",
            "code_station",
            "code_statut",
            "libelle_masse_eau",
            "libelle_qualification",
            "libelle_station",
            "type_entite_hydro",
            "code_troncon_hydro",
            "libelle_cours_eau",
            "libelle_sous_bassin",
            "libelle_statut",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in ("code_commune", "libelle_commune"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 50)
            except KeyError:
                continue

        for arg in ("code_departement", "libelle_departement"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in (
            "code_region",
            "libelle_region",
            "code_bassin",
            "libelle_bassin",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 10)
            except KeyError:
                continue

        for arg in (
            "distance",
            "latitude",
            "longitude",
            "resultat_min",
            "resultat_max",
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
        url = self.BASE_URL + "/v1/temperature/chronique"
        df = self.get_result(
            method,
            url,
            time_start="date_debut_mesure",
            time_end="date_fin_mesure",
            params=params,
        )

        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        except KeyError:
            pass

        return df
