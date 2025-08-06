# -*- coding: utf-8 -*-
"""
low level class to collect data from the fish API from
hub'eau
"""
import pandas as pd

from cl_hubeau.session import BaseHubeauSession
from cl_hubeau.exceptions import UnexpectedArguments


class FishSession(BaseHubeauSession):
    """
    Base session class to handle the fish API
    """

    DOC_URL = "http://hubeau.eaufrance.fr/page/api-poisson"

    def __init__(self, *args, **kwargs):

        super().__init__(version="v1", *args, **kwargs)

        # Set default size for API queries, based on hub'eau
        self.size = 5000

    def get_stations(self, **kwargs):
        """
        Lister les stations
        Endpoint /v1/etat_piscicole/stations

        Ce service permet de rechercher les stations

        Doc: http://hubeau.eaufrance.fr/page/api-poisson
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
            "code_bassin",
            "code_commune",
            "code_departement",
            "code_entite_hydrographique",
            "code_masse_eau",
            "code_point_prelevement",
            "code_point_prelevement_aspe",
            "code_regime_hydrologique",
            "code_region",
            "code_station",
            "codes_dispositifs_collecte",
            "libelle_commune",
            "libelle_departement",
            "libelle_entite_hydrographique",
            "libelle_regime_hydrologique",
            "libelle_region",
            "libelle_station",
            "libelles_dispositifs_collecte",
            "zone_huet",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in ("libelle_bassin",):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 15)
            except KeyError:
                continue

        for arg in ("objectifs_operation",):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in (
            "distance",
            "distance_source_max," "distance_source_min",
            "pente_max",
            "pente_min",
            "surface_bassin_versant_amont_max",
            "surface_bassin_versant_amont_min",
            "largeur_lit_mineur_max",
            "largeur_lit_mineur_min",
            "latitude",
            "longitude",
            "altitude_max",
            "altitude_min",
            "distance_mer_max",
            "distance_mer_min",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["donnees_cc"] = kwargs.pop("donnees_cc") in ("true", True)
        except KeyError:
            params["donnees_cc"] = "true"

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/etat_piscicole/stations"
        df = self.get_result(method, url, params=params)

        return df

    def get_observations(self, **kwargs):
        """
        Lister les observations
        Endpoint /v1/etat_piscicole/observations

        Ce service permet de récupérer les observations réalisées lors d'opérations
        de pêches scientifiques à l'électricité.
        Les données regroupent les informations relatives au prélèvement élémentaire,
        de ses caractéristiques (type, durée, ...)
        aux mesures individuelles (taxon, taille, poids, ...)

        Doc: http://hubeau.eaufrance.fr/page/api-poisson
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
            "date_operation_max",
            "date_operation_min",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_commune",
            "code_departement",
            "code_entite_hydrographique",
            "code_point_prelevement",
            "code_point_prelevement_aspe",
            "code_region",
            "code_station",
            "codes_dispositifs_collecte",
            "libelle_commune",
            "libelle_departement",
            "libelle_entite_hydrographique",
            "libelle_region",
            "libelle_station",
            "libelles_dispositifs_collecte",
            "code_operation",
            "code_taxon",
            "code_alternatif_taxon",
            "nom_commun_taxon",
            "nom_latin_taxon",
            "numero_passage",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_qualification_operation",
            "libelle_qualification_operation",
            "code_type_lot",
            "libelle_type_lot",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 5)
            except KeyError:
                continue

        for arg in (
            "code_bassin",
            "libelle_bassin",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 15)
            except KeyError:
                continue

        for arg in (
            "codes_pathologies_individu",
            "codes_pathologies_lot",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 60)
            except KeyError:
                continue

        for arg in (
            "distance",
            "latitude",
            "longitude",
            "nombre_points_max",
            "nombre_points_min",
            "poids_individu_mesure_max",
            "poids_individu_mesure_min",
            "poids_lot_mesure_max",
            "poids_lot_mesure_min",
            "taille_individu_max",
            "taille_individu_min",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:

            variable = self._ensure_val_among_authorized_values(
                "etat_avancement_operation",
                kwargs,
                {
                    "En cours de saisie",
                    "Validé niveau 1",
                    "Validé niveau 2",
                },
                str,
            )
        except KeyError:
            pass
        else:
            params["etat_avancement_operation"] = self.list_to_str_param(
                variable, 3
            )

        try:
            variable = self._ensure_val_among_authorized_values(
                "protocole_peche",
                kwargs,
                {
                    "Pêche complète à un ou plusieurs passages",
                    "Pêche partielle par points (grand milieu)",
                    "Pêche par ambiances",
                    "Pêche partielle sur berge",
                    "Indice Abondance Saumon",
                    "Vigitruite",
                    "Indice Abondance Anguille",
                },
                str,
            )
        except KeyError:
            pass
        else:
            params["protocole_peche"] = self.list_to_str_param(variable, 10)

        try:
            variable = self._ensure_val_among_authorized_values(
                "objectifs_operation",
                kwargs,
                {
                    "RCA - Réseau de contrôle additionnel",
                    "RCS - Réseau de contrôle de Surveillance",
                    "RRP - Réseau de référence Pérenne",
                    "RCO - Réseau Contrôle opérationnel",
                    "DCE - Référence",
                    "RHP - Réseau Hydrobiologique Piscicole",
                    "RNB - Réseau National de Bassin",
                    "RNSORMCE - Réseau National de Suivi des Opéra...",
                    "Étude",
                    "Suivi des cours d'eau intermittents",
                    "Suivi de restauration",
                    "Suivi des populations d'anguilles",
                    "Suivi des populations de saumons",
                    "Suivi des populations de truites",
                    "Sauvetage - Transfert",
                },
                str,
            )
        except KeyError:
            pass
        else:
            params["objectifs_operation"] = self.list_to_str_param(
                variable, 20
            )

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/etat_piscicole/observations"
        df = self.get_result(method, url, params=params)

        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        except KeyError:
            pass

        return df

    def get_indicators(self, **kwargs):
        """
        Lister les indicateurs
        Endpoint /v1/etat_piscicole/indicateurs

        Ce service permet de récupérer les indicateurs (IPR et IPR+)
        et métriques calculés par le SEEE à partir des observations
        et des données environnementales collectées lors des opérations de pêches à l'électricité.

        Doc: http://hubeau.eaufrance.fr/page/api-poisson
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
            "date_operation_max",
            "date_operation_min",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_bassin",
            "code_commune",
            "code_departement",
            "code_entite_hydrographique",
            "code_point_prelevement",
            "code_point_prelevement_aspe",
            "code_region",
            "code_station",
            "codes_dispositifs_collecte",
            "libelle_commune",
            "libelle_departement",
            "libelle_entite_hydrographique",
            "libelle_region",
            "libelle_station",
            "libelles_dispositifs_collecte",
            "objectifs_operation",
            "libelle_bassin",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_qualification_operation",
            "libelle_qualification_operation",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 5)
            except KeyError:
                continue

        for arg in "etat_avancement_operation":
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 5)
            except KeyError:
                continue

        for arg in (
            "distance",
            "latitude",
            "longitude",
            "ipr_note_max",
            "ipr_note_min",
            "iprplus_note_min",
            "iprplus_note_max",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:
            params["ipr_code_classe"] = (
                self._ensure_val_among_authorized_values(
                    "ipr_code_classe", kwargs, {"1", "2", "3", "4", "5"}, str
                )
            )
        except KeyError:
            pass

        try:
            params["iprplus_code_classe"] = (
                self._ensure_val_among_authorized_values(
                    "iprplus_code_classe",
                    kwargs,
                    {"1", "2", "3", "4", "5"},
                    str,
                )
            )
        except KeyError:
            pass

        try:
            params["iprplus_libelle_classe"] = (
                self._ensure_val_among_authorized_values(
                    "iprplus_libelle_classe",
                    kwargs,
                    {"1", "2", "3", "4", "5"},
                    str,
                )
            )
        except KeyError:
            pass

        try:
            params["ipr_libelle_classe"] = (
                self._ensure_val_among_authorized_values(
                    "ipr_libelle_classe",
                    kwargs,
                    {"Très bon", "Bon", "Moyen", "Médiocre", "Mauvais"},
                    str,
                )
            )
        except KeyError:
            pass

        try:
            params["protocole_peche"] = (
                self._ensure_val_among_authorized_values(
                    "protocole_peche",
                    kwargs,
                    {
                        "Pêche complète à un ou plusieurs passages",
                        "Pêche partielle par points (grand milieu)",
                        "Pêche par ambiances",
                        "Pêche partielle sur berge",
                        "Indice Abondance Saumon",
                        "Vigitruite",
                        "Indice Abondance Anguille",
                    },
                    str,
                )
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
        url = self.BASE_URL + "/v1/etat_piscicole/indicateurs"
        df = self.get_result(method, url, params=params)

        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        except KeyError:
            pass

        return df

    def get_operations(self, **kwargs):
        """
        Lister les operations
        Endpoint /v1/etat_piscicole/operations

        Ce service permet de récupérer les operations

        Doc: http://hubeau.eaufrance.fr/page/api-poisson
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
            "date_operation_max",
            "date_operation_min",
            "date_creation_operation_max",
            "date_creation_operation_min",
            "date_modification_operation_max",
            "date_modification_operation_min",
        ):
            try:
                variable = kwargs.pop(arg)
                self.ensure_date_format_is_ok(variable)
                params[arg] = variable
            except KeyError:
                continue

        for arg in (
            "code_commune",
            "code_departement",
            "code_entite_hydrographique",
            "code_point_prelevement",
            "code_point_prelevement_aspe",
            "code_region",
            "code_station",
            "codes_dispositifs_collecte",
            "libelle_commune",
            "libelle_departement",
            "libelle_entite_hydrographique",
            "libelle_region",
            "libelle_station",
            "libelles_dispositifs_collecte",
            "code_operation",
            "autres_especes_codes_alternatifs_taxon",
            "autres_especes_codes_taxon",
            "autres_especes_noms_communs_taxon",
            "autres_especes_noms_latins_taxon",
            "commanditaire_code",
            "commanditaire_libelle",
            "commanditaire_libelle_aspe",
            "espece_ciblee_code_alternatif_taxon",
            "espece_ciblee_code_taxon",
            "espece_ciblee_nom_commun_taxon",
            "espece_ciblee_nom_latin_taxon",
            "expert_technique_libelle",
            "expert_technique_libelle_aspe",
            "operateur_code",
            "operateur_libelle",
            "operateur_libelle_aspe",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 200)
            except KeyError:
                continue

        for arg in (
            "code_qualification_operation",
            "libelle_qualification_operation",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 5)
            except KeyError:
                continue

        for arg in (
            "code_bassin",
            "libelle_bassin",
        ):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 15)
            except KeyError:
                continue

        for arg in "expert_technique_code":
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(variable, 20)
            except KeyError:
                continue

        for arg in (
            "distance",
            "latitude",
            "longitude",
            "nombre_anodes_max",
            "nombre_anodes_min",
            "intensite_max",
            "intensite_min",
            "largeur_lame_eau_max",
            "largeur_lame_eau_min",
            "longueur_max",
            "longueur_min",
            "pente_ligne_eau_max",
            "pente_ligne_eau_min",
            "puissance_max",
            "puissance_min",
            "tension_max",
            "tension_min",
            "temperature_instantannee_max",
            "temperature_instantannee_min",
            "conductivite_max",
            "conductivite_min",
            "espece_ciblee",
            "operation_sans_poisson",
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        try:

            variable = self._ensure_val_among_authorized_values(
                "etat_avancement_operation",
                kwargs,
                {
                    "En cours de saisie",
                    "Validé niveau 1",
                    "Validé niveau 2",
                },
                str,
            )
        except KeyError:
            pass
        else:
            params["etat_avancement_operation"] = self.list_to_str_param(
                variable, 3
            )

        try:
            variable = self._ensure_val_among_authorized_values(
                "protocole_peche",
                kwargs,
                {
                    "Pêche complète à un ou plusieurs passages",
                    "Pêche partielle par points (grand milieu)",
                    "Pêche par ambiances",
                    "Pêche partielle sur berge",
                    "Indice Abondance Saumon",
                    "Vigitruite",
                    "Indice Abondance Anguille",
                },
                str,
            )
        except KeyError:
            pass
        else:
            params["protocole_peche"] = self.list_to_str_param(variable, 10)

        try:
            variable = self._ensure_val_among_authorized_values(
                "objectifs_operation",
                kwargs,
                {
                    "RCA - Réseau de contrôle additionnel",
                    "RCS - Réseau de contrôle de Surveillance",
                    "RRP - Réseau de référence Pérenne",
                    "RCO - Réseau Contrôle opérationnel",
                    "DCE - Référence",
                    "RHP - Réseau Hydrobiologique Piscicole",
                    "RNB - Réseau National de Bassin",
                    "RNSORMCE - Réseau National de Suivi des Opéra...",
                    "Étude",
                    "Suivi des cours d'eau intermittents",
                    "Suivi de restauration",
                    "Suivi des populations d'anguilles",
                    "Suivi des populations de saumons",
                    "Suivi des populations de truites",
                    "Sauvetage - Transfert",
                },
                str,
            )
        except KeyError:
            pass
        else:
            params["objectifs_operation"] = self.list_to_str_param(
                variable, 20
            )

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/etat_piscicole/operations"
        df = self.get_result(method, url, params=params)

        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        except KeyError:
            pass

        return df


# if __name__ == "__main__":
#     session = FishSession()
#     df = session.get_observations(
#         code_station="06165000",
#         format="geojson",
#     )
