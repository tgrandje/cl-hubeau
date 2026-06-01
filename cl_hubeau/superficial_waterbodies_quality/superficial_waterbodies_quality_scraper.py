# -*- coding: utf-8 -*-
"""
low level class to collect data from the drinking water quality API from
hub'eau
"""
from deprecated import deprecated
import pandas as pd

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class SuperficialWaterbodiesQualitySession(BaseHubeauSession):
    """
    Base session class to handle the superifical waterbodies' quality API
    """

    DOC_URL = "https://hubeau.eaufrance.fr/page/api-qualite-cours-deau"

    def __init__(self, *args, **kwargs):

        super().__init__(version="2.0.0", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 5000

    def get_stations(self, **kwargs):
        """
        Lister les stations de mesures physicochimique
        Endpoint /v2/qualite_rivieres/station_pc

        Ce service permet de rechercher des stations de mesures physicochimique
        sur des cours d'eau et plan d'eau en France et les DROM.

        Doc: https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
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
            "date_debut_prelevement",
            "date_fin_maj",
            "date_fin_prelevement",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_banque_reference",
            "code_bassin_dce",
            "code_commune",
            "code_cours_eau",
            "code_departement",
            "code_eu_masse_eau",
            "code_fraction",
            "code_groupe_parametres",
            "code_masse_eau",
            "code_parametre",
            "code_qualification",
            "code_region",
            "code_reseau",
            "code_sous_bassin",
            "code_station",
            "code_statut",
            "code_support",
            "libelle_commune",
            "libelle_departement",
            "libelle_fraction",
            "libelle_masse_eau",
            "libelle_parametre",
            "libelle_qualification",
            "libelle_region",
            "libelle_reseau",
            "libelle_station",
            "libelle_support",
            "mnemo_statut",
            "nom_bassin_dce",
            "nom_cours_eau",
            "nom_groupe_parametres",
            "nom_sous_bassin",
            "type_entite_hydro",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
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
        url = self.BASE_URL + "/v2/qualite_rivieres/station_pc"
        df = self.get_result(method, url, params=params)

        return df

    def get_operations(self, **kwargs):
        """
        Lister les opérations physicochimique
        Endpoint /v2/qualite_rivieres/operation_pc

        Ce service permet de rechercher des opérations physicochimiques sur des
        cours d'eau et plan d'eau en France et les DROM.

        Doc: https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
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
            "date_debut_prelevement",
            "date_fin_maj",
            "date_fin_prelevement",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_banque_reference",
            "code_bassin_dce",
            "code_commune",
            "code_cours_eau",
            "code_departement",
            "code_eu_masse_eau",
            "code_fraction",
            "code_groupe_parametres",
            "code_masse_eau",
            "code_parametre",
            "code_prelevement",
            "code_qualification",
            "code_region",
            "code_reseau",
            "code_sous_bassin",
            "code_station",
            "code_statut",
            "code_support",
            "libelle_commune",
            "libelle_departement",
            "libelle_fraction",
            "libelle_masse_eau",
            "libelle_parametre",
            "libelle_qualification",
            "libelle_region",
            "libelle_reseau",
            "libelle_station",
            "libelle_support",
            "mnemo_statut",
            "nom_bassin_dce",
            "nom_cours_eau",
            "nom_groupe_parametres",
            "nom_sous_bassin",
            "type_entite_hydro",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
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
        url = self.BASE_URL + "/v2/qualite_rivieres/operation_pc"
        df = self.get_result(
            method,
            url,
            time_start="date_debut_prelevement",
            time_end="date_fin_prelevement",
            params=params,
        )

        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        except KeyError:
            pass

        return df

    def get_environmental_conditions(self, **kwargs):
        """
        Lister les conditions environnementales physicochimique
        Endpoint /v2/qualite_rivieres/condition_environnementale_pc

        Ce service permet de rechercher des conditions environnementales
        physicochimique d'analyses (attention la pagination est basée sur le
        nombre d'analyse) sur des cours d'eau et plan d'eau en France et les
        DROM.

        Doc: https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
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
            "date_debut_prelevement",
            "date_fin_maj",
            "date_fin_prelevement",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_banque_reference",
            "code_commune",
            "code_cours_eau",
            "code_departement",
            "code_eu_masse_eau",
            "code_groupe_parametres",
            "code_masse_eau",
            "code_parametre",
            "code_prelevement",
            "code_qualification",
            "code_region",
            "code_station",
            "code_statut",
            "libelle_commune",
            "libelle_departement",
            "libelle_masse_eau",
            "libelle_parametre",
            "libelle_qualification",
            "libelle_region",
            "libelle_station",
            "mnemo_statut",
            "nom_cours_eau",
            "nom_groupe_parametres",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in ("distance", "latitude", "longitude"):
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
        url = (
            self.BASE_URL
            + "/v2/qualite_rivieres/condition_environnementale_pc"
        )
        df = self.get_result(
            method,
            url,
            time_start="date_debut_prelevement",
            time_end="date_fin_prelevement",
            params=params,
        )

        try:
            df["date_prelevement"] = pd.to_datetime(
                df["date_prelevement"], format="%Y-%m-%d"
            )
        except KeyError:
            pass

        return df

    @deprecated(
        version="0.6.0",
        reason=(
            "Please use `SuperficialWaterbodiesQualitySession.get_analyses` "
            "instead.",
        ),
    )
    def get_analysis(self, **kwargs):
        """
        Lister les analyses physicochimique
        Endpoint /v2/qualite_rivieres/analyse_pc

        Ce service permet de rechercher des analyses physicochimique sur des
        cours d'eau et plan d'eau en France et les DROM.

        Doc: https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
        """
        return self.get_analyses(**kwargs)

    def get_analyses(self, **kwargs):
        """
        Lister les analyses physicochimique
        Endpoint /v2/qualite_rivieres/analyse_pc

        Ce service permet de rechercher des analyses physicochimique sur des
        cours d'eau et plan d'eau en France et les DROM.

        Doc: https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
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
            "date_debut_prelevement",
            "date_fin_maj",
            "date_fin_prelevement",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_banque_reference",
            "code_bassin_dce",
            "code_commune",
            "code_cours_eau",
            "code_departement",
            "code_eu_masse_eau",
            "code_fraction",
            "code_groupe_parametres",
            "code_masse_eau",
            "code_parametre",
            "code_prelevement",
            "code_qualification",
            "code_region",
            "code_reseau",
            "code_sous_bassin",
            "code_station",
            "code_statut",
            "code_support",
            "libelle_commune",
            "libelle_departement",
            "libelle_fraction",
            "libelle_masse_eau",
            "libelle_parametre",
            "libelle_qualification",
            "libelle_region",
            "libelle_reseau",
            "libelle_station",
            "libelle_support",
            "mnemo_statut",
            "nom_bassin_dce",
            "nom_cours_eau",
            "nom_groupe_parametres",
            "nom_sous_bassin",
            "type_entite_hydro",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in ("distance", "latitude", "longitude"):
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
        url = self.BASE_URL + "/v2/qualite_rivieres/analyse_pc"
        df = self.get_result(
            method,
            url,
            time_start="date_debut_prelevement",
            time_end="date_fin_prelevement",
            params=params,
        )

        try:
            df["date_prelevement"] = pd.to_datetime(
                df["date_prelevement"], format="%Y-%m-%d"
            )
        except KeyError:
            pass

        # optimize to categorical, those dataframes are heavy
        for x in df.loc[:, df.dtypes == "object"]:
            try:
                df[x] = pd.Categorical(df[x])
            except TypeError:
                pass

        return df
