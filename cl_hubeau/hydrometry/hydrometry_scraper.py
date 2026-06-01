# -*- coding: utf-8 -*-
"""
low level class to collect data from the hydrometry API from hub'eau
"""

import pandas as pd

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class HydrometrySession(BaseHubeauSession):
    """
    Base session class to handle the hydrometry API
    """

    DOC_URL = "https://hubeau.eaufrance.fr/page/api-hydrometrie"

    def __init__(self, *args, **kwargs):

        super().__init__(version="2.0.1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 1000

    def get_stations(self, **kwargs):
        """
        Lister les stations hydrométriques
        Endpoint /api/v2/hydrometrie/referentiel/stations

        Ce service permet d'interroger les stations du référentiel
        hydrométrique.
        Une station peut porter des observations de hauteur et/ou de débit
        (directement mesurés ou calculés à partir d'une courbe de tarage).
        Si la valeur du paramètre size n'est pas renseignée, la taille de page
        par défaut : 1000, taille max de la page : 10000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        Trie par défaut : code_station asc

        Doc: https://hubeau.eaufrance.fr/page/api-hydrometrie
        """

        params = {}
        for arg in ("date_fermeture_station", "date_ouverture_station"):
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
            params["en_service"] = self._ensure_val_among_authorized_values(
                "en_service", kwargs, {0, 1}, int
            )
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in (
            "code_commune_station",
            "code_cours_eau",
            "code_departement",
            "code_region",
            "code_sandre_reseau_station",
            "code_site",
            "code_station",
            "fields",
            "libelle_cours_eau",
            "libelle_site",
            "libelle_station",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        for arg in ("distance", "latitude", "longitude"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v2/hydrometrie/referentiel/stations"
        df = self.get_result(method, url, params=params)

        for f in (
            "date_maj_ref_alti_station",
            "date_fermeture_station",
            "date_activation_ref_alti_station",
            "date_debut_ref_alti_station",
            "date_ouverture_station",
            "date_maj_station",
        ):
            try:
                df[f] = pd.to_datetime(df[f])
            except KeyError:
                continue

        return df

    def get_sites(self, **kwargs):
        """
        Lister les sites hydrométriques
        Endpoint /api/v2/hydrometrie/referentiel/sites

        Ce service permet d'interroger les sites du référentiel hydrométrique
        (tronçon de cours d'eau sur lequel les mesures de débit sont réputées
         homogènes et comparables entre elles). Un site peut posséder une ou
        plusieurs stations ; il est support de données de débit (Q)
        Si la valeur du paramètre size n'est pas renseignée, la taille de page
        par défaut : 1000, taille max de la page : 10000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        Trie par défaut : code_site asc

        Doc: https://hubeau.eaufrance.fr/page/api-hydrometrie
        """

        params = {}

        try:
            params["format"] = self._ensure_val_among_authorized_values(
                "format", kwargs, {"json", "geojson"}
            )
        except KeyError:
            pass

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        for arg in (
            "code_commune_site",
            "code_cours_eau",
            "code_departement",
            "code_region",
            "code_site",
            "code_troncon_hydro_site",
            "code_zone_hydro_site",
            "fields",
            "libelle_cours_eau",
            "libelle_site",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        for arg in ("distance", "latitude", "longitude"):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v2/hydrometrie/referentiel/sites"
        df = self.get_result(method, url, params=params)

        for f in (
            "date_premiere_donnee_dispo_site",
            "date_maj_site",
        ):
            try:
                df[f] = pd.to_datetime(df[f])
            except KeyError:
                continue

        return df

    def get_observations(self, **kwargs):
        """
        Lister les observations hydrométriques élaborées
        Endpoint /api/v2/hydrometrie/obs_elab

        Grandeurs hydrométriques élaborées disponibles : débits moyens
        journaliers (QmnJ), débits moyens mensuels (QmM), Hauteur instantanée
        maximale mensuelle (HIXM), Hauteur instantanée maximale
        journalière (HIXnJ), Débit instantané minimal mensuel (QINM), Débit
        instantané minimal journalier (QINnJ), Débit instantané maximal
        mensuel (QixM), Débit instantané maximal journalier (QIXnJ)
        Si la valeur du paramètre size n'est pas renseignée, la taille de page
        par défaut : 1000, taille max de la page : 20000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        Trie par défaut : code_station,date_obs_elab asc

        Doc: https://hubeau.eaufrance.fr/page/api-hydrometrie
        """

        params = {}

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        try:
            params["grandeur_hydro_elab"] = (
                self._ensure_val_among_authorized_values(
                    "grandeur_hydro_elab",
                    kwargs,
                    {
                        "QmnJ",
                        "QmM",
                        "HIXM",
                        "HIXnJ",
                        "QINM",
                        "QINnJ",
                        "QixM",
                        "QIXnJ",
                    },
                )
            )
        except KeyError:
            pass

        try:
            variable = kwargs.pop("code_entite")
            params["code_entite"] = self.list_to_str_param(variable, 100)
        except KeyError:
            pass

        try:
            variable = kwargs.pop("fields")
            params["fields"] = self.list_to_str_param(variable)
        except KeyError:
            pass

        for arg in ("date_debut_obs_elab", "date_fin_obs_elab"):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "distance",
            "latitude",
            "longitude",
            "resultat_max",
            "resultat_min",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v2/hydrometrie/obs_elab"

        df = self.get_result(
            method,
            url,
            time_start="date_debut_obs_elab",
            time_end="date_fin_obs_elab",
            params=params,
        )

        for f in "date_obs_elab", "date_prod":
            try:
                df[f] = pd.to_datetime(df[f])
            except KeyError:
                continue

        return df

    def get_realtime_observations(self, **kwargs):
        """
        Lister les observations hydrométriques
        Endpoint /api/v2/hydrometrie/observations_tr

        Ce service permet de lister les observations dites "temps réel" portées
        par le référentiel (sites et stations hydrométriques), à savoir les
        séries de données de hauteur d'eau (H) et de débit (Q).
        Si la valeur du paramètre size n'est pas renseignée, la taille de page
        par défaut : 1000, taille max de la page : 20000.
        Il n'y a pas de limitation sur la profondeur d'accès aux résultats.
        Trie par défaut : date_obs desc

        Doc: https://hubeau.eaufrance.fr/page/api-hydrometrie
        """

        params = {}

        try:
            params["bbox"] = self.list_to_str_param(
                kwargs.pop("bbox"), None, 4
            )
        except KeyError:
            pass

        try:
            params["sort"] = self._ensure_val_among_authorized_values(
                "sort", kwargs, {"asc", "desc"}
            )
        except KeyError:
            pass

        try:
            params["grandeur_hydro"] = (
                self._ensure_val_among_authorized_values(
                    "grandeur_hydro", kwargs, {"H", "Q"}
                )
            )
        except KeyError:
            pass

        for arg in ("code_entite", "fields", "code_statut"):
            try:
                variable = kwargs.pop(arg)
                authorized_values = None
                if arg == "code_statut":
                    authorized_values = {0, 4, 8, 12, 16}
                params[arg] = self.list_to_str_param(
                    variable, authorized_values=authorized_values
                )
            except KeyError:
                continue

        try:
            params["timestep"] = kwargs.pop("timestep")
            try:
                if len(",".split(params["code_entite"])) > 1:
                    raise ValueError(
                        "timestep can only be set for one 'code_entite', "
                        f"found code_entite='{params['code_entite']}' instead"
                    )
            except KeyError as exc:
                raise ValueError(
                    "timestep can only be set for one 'code_entite', "
                    "found code_entite=None instead"
                ) from exc

        except KeyError:
            pass

        for arg in ("date_debut_obs", "date_fin_obs"):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "distance",
            "latitude",
            "longitude",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v2/hydrometrie/observations_tr"

        df = self.get_result(
            method,
            url,
            time_start="date_debut_obs",
            time_end="date_fin_obs",
            params=params,
        )

        for f in "date_debut_serie", "date_fin_serie", "date_obs":
            try:
                df[f] = pd.to_datetime(df[f])
            except KeyError:
                continue

        return df
