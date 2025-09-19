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
                params[arg] = self.list_to_str_param(
                    variable,
                    20,
                    authorized_values=[
                        "RCA - Réseau de contrôle additionnel",
                        "RCS – Réseau de Contrôle de Surveillance",
                        "RRP – Réseau de Référence Pérenne",
                        "RCO – Réseau Contrôle opérationnel",
                        "DCE – Référence",
                        "RHP – Réseau Hydrobiologique Piscicole",
                        "RNB – Réseau National de Bassin",
                        "RNSORMCE – Réseau National de Suivi des Opérations de Restauration hydroMorphologiques des Cours d'Eau",
                        "Étude",
                        "Suivi des cours d'eau intermittents",
                        "Suivi de restauration",
                        "Suivi des populations d'anguilles",
                        "Suivi des populations de saumons",
                        "Suivi des populations de truites",
                        "Sauvetage - Transfert",
                    ],
                )
            except KeyError:
                continue

        for arg in (
            "distance",
            "distance_source_max",
            "distance_source_min",
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
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/etat_piscicole/stations"
        df = self.get_result(method, url, params=params)

        for x in [
            "date_modification_station",
            "date_modification_point_prelevement_aspe",
        ]:
            try:
                df[x] = pd.to_datetime(df[x])
            except KeyError:
                pass

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
            params["etat_avancement_operation"] = self.list_to_str_param(
                kwargs.pop("etat_avancement_operation"),
                3,
                authorized_values=[
                    "En cours de saisie",
                    "Validé niveau 1",
                    "Validé niveau 2",
                ],
            )
        except KeyError:
            pass

        try:
            params["protocole_peche"] = self.list_to_str_param(
                kwargs.pop("protocole_peche"),
                10,
                authorized_values=[
                    "Pêche complète à un ou plusieurs passages",
                    "Pêche partielle par points (grand milieu)",
                    "Pêche par ambiances",
                    "Pêche partielle sur berge",
                    "Indice Abondance Saumon",
                    "Vigitruite",
                    "Indice Abondance Anguille",
                ],
            )

        except KeyError:
            pass

        for arg in ("objectifs_operation",):
            try:
                variable = kwargs.pop(arg)
                params[arg] = self.list_to_str_param(
                    variable,
                    20,
                    authorized_values=[
                        "RCA - Réseau de contrôle additionnel",
                        "RCS – Réseau de Contrôle de Surveillance",
                        "RRP – Réseau de Référence Pérenne",
                        "RCO – Réseau Contrôle opérationnel",
                        "DCE – Référence",
                        "RHP – Réseau Hydrobiologique Piscicole",
                        "RNB – Réseau National de Bassin",
                        "RNSORMCE – Réseau National de Suivi des Opérations de Restauration hydroMorphologiques des Cours d'Eau",
                        "Étude",
                        "Suivi des cours d'eau intermittents",
                        "Suivi de restauration",
                        "Suivi des populations d'anguilles",
                        "Suivi des populations de saumons",
                        "Suivi des populations de truites",
                        "Sauvetage - Transfert",
                    ],
                )
            except KeyError:
                continue

        try:
            params["fields"] = self.list_to_str_param(kwargs.pop("fields"))
        except KeyError:
            pass

        if kwargs:
            raise UnexpectedArguments(kwargs, self.DOC_URL)

        method = "GET"
        url = self.BASE_URL + "/v1/etat_piscicole/observations"
        df = self.get_result(
            method,
            url,
            params=params,
            time_start="date_operation_min",
            time_end="date_operation_max",
        )

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

        # TODO

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
            "code_operation",
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

        try:
            params["objectifs_operation"] = self.list_to_str_param(
                kwargs.pop("objectifs_operation"),
                20,
                authorized_values=[
                    "RCA - Réseau de contrôle additionnel",
                    "RCS – Réseau de Contrôle de Surveillance",
                    "RRP – Réseau de Référence Pérenne",
                    "RCO – Réseau Contrôle opérationnel",
                    "DCE – Référence",
                    "RHP – Réseau Hydrobiologique Piscicole",
                    "RNB – Réseau National de Bassin",
                    "RNSORMCE – Réseau National de Suivi des Opérations de Restauration hydroMorphologiques des Cours d'Eau",
                    "Étude",
                    "Suivi des cours d'eau intermittents",
                    "Suivi de restauration",
                    "Suivi des populations d'anguilles",
                    "Suivi des populations de saumons",
                    "Suivi des populations de truites",
                    "Sauvetage - Transfert",
                ],
            )
        except KeyError:
            pass

        try:
            params["etat_avancement_operation"] = self.list_to_str_param(
                kwargs.pop("etat_avancement_operation"),
                3,
                authorized_values=[
                    "En cours de saisie",
                    "Validé niveau 1",
                    "Validé niveau 2",
                ],
            )
        except KeyError:
            pass

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
            for arg in "ipr_code_classe", "iprplus_code_classe":
                params[arg] = self.list_to_str_param(
                    kwargs.pop(arg),
                    authorized_values=["1", "2", "3", "4", "5"],
                )
        except KeyError:
            pass

        try:
            for arg in "ipr_libelle_classe", "iprplus_libelle_classe":
                params[arg] = self.list_to_str_param(
                    kwargs.pop(arg),
                    authorized_values=[
                        "Très bon",
                        "Bon",
                        "Moyen",
                        "Médiocre",
                        "Mauvais",
                    ],
                )
        except KeyError:
            pass

        try:
            params["protocole_peche"] = self.list_to_str_param(
                kwargs.pop(arg),
                authorized_values=[
                    "Pêche complète à un ou plusieurs passages",
                    "Pêche partielle par points (grand milieu)",
                    "Pêche par ambiances",
                    "Pêche partielle sur berge",
                    "Indice Abondance Saumon",
                    "Vigitruite",
                    "Indice Abondance Anguille",
                ],
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
        df = self.get_result(
            method,
            url,
            params=params,
            time_start="date_operation_min",
            time_end="date_operation_max",
        )

        for x in [
            "date_operation",
            "ipr_date_execution",
            "iprplus_date_execution",
        ]:
            try:
                df[x] = pd.to_datetime(df[x])
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
            #
        ):
            try:
                params[arg] = kwargs.pop(arg)
            except KeyError:
                continue

        for arg in ["espece_ciblee", "operation_sans_poisson"]:
            try:
                params[arg] = kwargs.pop(arg) in ("true", True)
            except KeyError:
                pass

        try:
            params["etat_avancement_operation"] = self.list_to_str_param(
                kwargs.pop("etat_avancement_operation"),
                3,
                authorized_values=[
                    "En cours de saisie",
                    "Validé niveau 1",
                    "Validé niveau 2",
                ],
            )
        except KeyError:
            pass

        try:
            params["protocole_peche"] = self.list_to_str_param(
                kwargs.pop("protocole_peche"),
                10,
                authorized_values=[
                    "Pêche complète à un ou plusieurs passages",
                    "Pêche partielle par points (grand milieu)",
                    "Pêche par ambiances",
                    "Pêche partielle sur berge",
                    "Indice Abondance Saumon",
                    "Vigitruite",
                    "Indice Abondance Anguille",
                ],
            )
        except KeyError:
            pass

        try:
            params["objectifs_operation"] = self.list_to_str_param(
                kwargs.pop("objectifs_operation"),
                20,
                authorized_values=[
                    "RCA - Réseau de contrôle additionnel",
                    "RCS – Réseau de Contrôle de Surveillance",
                    "RRP – Réseau de Référence Pérenne",
                    "RCO – Réseau Contrôle opérationnel",
                    "DCE – Référence",
                    "RHP – Réseau Hydrobiologique Piscicole",
                    "RNB – Réseau National de Bassin",
                    "RNSORMCE – Réseau National de Suivi des Opérations de Restauration hydroMorphologiques des Cours d'Eau",
                    "Étude",
                    "Suivi des cours d'eau intermittents",
                    "Suivi de restauration",
                    "Suivi des populations d'anguilles",
                    "Suivi des populations de saumons",
                    "Suivi des populations de truites",
                    "Sauvetage - Transfert",
                ],
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
        url = self.BASE_URL + "/v1/etat_piscicole/operations"
        df = self.get_result(
            method,
            url,
            params=params,
            time_start="date_operation_min",
            time_end="date_operation_max",
        )

        for x in [
            "date_creation_operation",
            "date_operation",
            "date_modification_operation",
        ]:
            try:
                df[x] = pd.to_datetime(df[x])
            except KeyError:
                pass

        return df


if __name__ == "__main__":
    with FishSession() as session:
        df = session.get_observations(code_taxon="2220")
