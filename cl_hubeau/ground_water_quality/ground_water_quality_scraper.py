# -*- coding: utf-8 -*-
"""
low level class to collect data from the ground water quality API from hub'eau
"""

import pandas as pd

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class GroundWaterQualitySession(BaseHubeauSession):
    """
    Base session class to handle the ground water quality API
    """

    DOC_URL = "http://hubeau.eaufrance.fr/page/api-qualite-nappes"

    def __init__(self, *args, **kwargs):
        super().__init__(version="1.2.1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 5000

    def get_stations(self, **kwargs):
        """
        lister les stations de mesure
        Endpoint /v1/qualite_nappes/stations

        Ce service permet de rechercher les stations de mesure des qualités des
        nappes d'eau.
        Source de données : banque nationale d'Accès aux Données sur les Eaux
        Souterraines (ADES) http://www.ades.eaufrance.fr/
        La taille de page par défaut : 5000, taille max de la page : 20000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.

        Doc: http://hubeau.eaufrance.fr/page/api-qualite-nappes
        """

        params = {}

        for arg in ("date_max_maj", "date_min_maj"):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue
        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            pass

        try:
            params["srid"] = self._ensure_val_among_authorized_values(
                "srid", kwargs, {4326, 900913, 3857}, int
            )
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in ("prof_invest_min", "prof_invest_max"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        for arg in (
            "bassin_dce",
            "nom_region",
            "nom_reseau",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 50)
            except KeyError:
                continue

        try:
            variable = kwargs.pop("circonscription_administrative_bassin")
            params["circonscription_administrative_bassin"] = (
                self.list_to_str_param(variable, 20)
            )
        except KeyError:
            pass

        for arg in (
            "bss_id",
            "code_entite_hg_bdlisa",
            "code_commune",
            "codes_masse_eau_edl",
            "code_masse_eau_rap",
            "nom_entite_hg_bdlisa",
            "nom_masse_eau_edl",
            "nom_masse_eau_rap",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        try:
            variable = kwargs.pop("num_departement")
            params["num_departement"] = self.list_to_str_param(variable, 100)
        except KeyError:
            pass

        try:
            variable = kwargs.pop("fields")
            params["fields"] = self.list_to_str_param(variable)
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/qualite_nappes/stations"
        df = self.get_result(method, url, params=params)

        for f in ["date_fin_mesure", "date_debut_mesure"]:
            try:
                df[f] = pd.to_datetime(df[f], errors="coerce")
            except KeyError:
                continue

        return df

    def get_analyses(self, **kwargs):
        """
        Lister les analyses
        Endpoint /v1/qualite_nappes/analyses

        Ce service permet de lister les mesures de qualité d'une station.
        Source de données : banque nationale d'Accès aux Données sur les Eaux
        Souterraines (ADES) http://www.ades.eaufrance.fr/
        La taille de page par défaut : 5000, taille max de la page : 20000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        """

        params = {}

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in (
            "date_debut_prelevement",
            "date_fin_prelevement",
            "date_max_maj",
            "date_min_maj",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "resultat_min",
            "resultat_max",
            "code_lieu_analyse",
            "code_type_point_eau",
            "code_unite",
            "nom_lieu_analyse",
            "nom_type_point_eau",
            "nom_unite",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        for arg in (
            "code_bassin_dce",
            "code_region",
            "code_reseau",
            "nom_bassin_dce",
            "nom_region",
            "nom_reseau",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 50)
            except KeyError:
                continue

        for arg in (
            "code_circonscription_administrative_bassin",
            "code_producteur",
            "nom_circonscription_administrative_bassin",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in (
            "bss_id",
            "code_entite_hg_bdlisa",
            "code_insee_actuel",
            "code_masse_eau_edl",
            "code_masse_eau_rap",
            "code_param",
            "nom_commune_actuel",
            "nom_masse_eau_edl",
            "nom_masse_eau_rap",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_fraction",
            "nom_fraction",
            "code_methode",
            "code_qualification",
            "code_remarque_analyse",
            "code_statut_analyse",
            "nom_methode",
            "nom_qualification",
            "nom_remarque_analyse",
            "nom_statut_analyse",
            "nom_entite_hg_bdlisa",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 10)
            except KeyError:
                continue

        for arg in (
            "code_groupe_parametre",
            "nom_departement",
            "nom_groupe_parametre",
            "num_departement",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 100)
            except KeyError:
                continue

        for arg in (
            "code_type_qualito",
            "nom_type_qualito",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 2)
            except KeyError:
                continue

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            pass

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/qualite_nappes/analyses"
        df = self.get_result(
            method,
            url,
            time_start="date_debut_prelevement",
            time_end="date_fin_prelevement",
            params=params,
        )

        for f in ["date_debut_prelevement"]:
            try:
                df[f] = pd.to_datetime(df[f], errors="coerce")
            except KeyError:
                continue

        return df
