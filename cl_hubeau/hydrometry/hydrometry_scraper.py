# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 14:03:41 2024

low level class to collect data from the hydrometry API from hub'eau
"""

import pandas as pd

from cl_hubeau.session import BaseHubeauSession


class HydrometrySession(BaseHubeauSession):
    """
    Base session class to handle the hydrometry API
    """

    def __init__(self, *args, **kwargs):

        super().__init__(version="1.0.1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau piezo's doc
        self.size = 1000

    def get_stations(self, **kwargs):
        """
        Lister les stations hydrométriques
        Endpoint /v1/hydrometrie/referentiel/stations

        Ce service permet d'interroger les stations du référentiel
        hydrométrique. Une station peut porter des observations de hauteur
        et/ou de débit (directement mesurés ou calculés à partir d'une courbe
        de tarage).
        Si la valeur du paramètre size n'est pas renseignée, la taille de page
        par défaut : 1000, taille max de la page : 10000.
        La profondeur d'accès aux résultats est : 20000, calcul de la
        profondeur = numéro de la page * nombre maximum de résultats dans une
        page.
        Trie par défaut : code_station asc

        Doc: https://hubeau.eaufrance.fr/page/api-hydrometrie
        """

        params = {}
        for arg in "date_fermeture_station", "date_ouverture_station":
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        try:
            variable = kwargs.pop("format")
            if variable not in ("json", "geojson"):
                raise ValueError(
                    "format must be among ('json', 'geojson'), "
                    f"found format='{variable}' instead"
                )
            params["format"] = variable
        except KeyError:
            pass

        try:
            params["en_service"] = int(kwargs.pop("en_service"))
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
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-hydrometrie"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/hydrometrie/referentiel/stations"
        df = self.get_result(method, url, params=params)

        return df

    def get_sites(self, **kwargs):
        """
        Lister les sites hydrométriques
        Endpoint /v1/hydrometrie/referentiel/sites

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
            variable = kwargs.pop("format")
            if variable not in ("json", "geojson"):
                raise ValueError(
                    "format must be among ('json', 'geojson'), "
                    f"found format='{variable}' instead"
                )
            params["format"] = variable
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
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-hydrometrie"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/hydrometrie/referentiel/sites"
        df = self.get_result(method, url, params=params)

        return df

    def get_observations(self, **kwargs):
        """
        Lister les observations hydrométriques élaborées
        Endpoint /v1/hydrometrie/obs_elab

        Grandeurs hydrométriques élaborées disponibles : débits moyens
        journaliers (QmJ), débits moyens mensuels (QmM)
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
            variable = kwargs.pop("grandeur_hydro_elab")
            if variable not in ("QmJ", "QmM"):
                raise ValueError(
                    "grandeur_hydro_elab must be among ('QmJ', 'QmM'), "
                    f"found grandeur_hydro_elab='{variable}' instead"
                )
            params["grandeur_hydro_elab "] = variable
        except KeyError:
            pass

        for arg in ("code_entite", "fields"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        for arg in "date_debut_obs_elab", "date_fin_obs_elab":
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
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-hydrometrie"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/hydrometrie/obs_elab"

        df = self.get_result(method, url, params=params)

        try:
            df["date_obs_elab"] = pd.to_datetime(
                df["date_obs_elab"], format="%Y-%m-%d"
            )
        except KeyError:
            pass
        return df

    def get_realtime_observations(self, **kwargs):
        """
        Lister les observations hydrométriques
        Endpoint /v1/hydrometrie/observations_tr

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
            variable = kwargs.pop("grandeur_hydro_elab")
            if variable not in ("QmJ", "QmM"):
                raise ValueError(
                    "grandeur_hydro_elab must be among ('QmJ', 'QmM'), "
                    f"found grandeur_hydro_elab='{variable}' instead"
                )
            params["grandeur_hydro_elab "] = variable
        except KeyError:
            pass

        try:
            variable = kwargs.pop("sort")
            if variable not in ("asc", "desc"):
                raise ValueError(
                    "sort must be among ('asc', 'desc'), "
                    f"found sort='{variable}' instead"
                )
            params["sort"] = variable
        except KeyError:
            pass

        try:
            variable = kwargs.pop("grandeur_hydro")
            if variable not in ("H", "Q"):
                raise ValueError(
                    "grandeur_hydro must be among ('H', 'Q'), "
                    f"found grandeur_hydro='{variable}' instead"
                )
            params["grandeur_hydro"] = variable
        except KeyError:
            pass

        for arg in ("code_entite", "fields"):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable)
            except KeyError:
                continue

        try:
            params["timestep"] = kwargs.pop("timestep")
            try:
                if len(",".split(params["code_entite"])) > 0:
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

        for arg in "date_debut_obs", "date_fin_obs":
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
            raise ValueError(
                f"found unexpected arguments {kwargs}, "
                "please have a look at the documentation on "
                "https://hubeau.eaufrance.fr/page/api-hydrometrie"
            )

        method = "GET"
        url = self.BASE_URL + "/v1/hydrometrie/observations_tr"

        df = self.get_result(method, url, params=params)

        try:
            df["date_obs"] = pd.to_datetime(df["date_obs"])
        except KeyError:
            pass
        return df


# if __name__ == "__main__":
#     import logging

#     # logging.basicConfig(level=logging.WARNING)
#     with HydrometrySession() as session:
#         gdf = session.get_sites(code_departement="02", format="geojson")
#         # df = session.get_observations(code_entite="K437311001")

#         df = session.get_realtime_observations(
#             code_entite="K437311001",
#             grandeur_hydro="Q",
#             # date_debut_obs="2010-01-01",
#         )
#         df.pivot_table(
#             index="date_obs", columns="grandeur_hydro", values="resultat_obs"
#         ).plot()
