---
layout: default
title: API Qualité des cours d'eau
language: fr
handle: /drinking-water-quality
nav_order: 14

---
# API Qualité des cours d'eau

[https://hubeau.eaufrance.fr/page/api-qualite-cours-deau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau)

`cl-hubeau` définit :

* des fonctions de haut niveau implémentant des boucles basiques ;
* des fonctions de bas niveau qui implémentent directement les différents points d'entrée de l'API.

{: .warning }
Lors de l'utilisation des fonctions de bas niveau, l'utilisateur est responsable
de la consommation de l'API. En particulier, il s'agit d'être vigilant quant au seuil
de 20 000 résultats récupérables d'une seule requête.
Ce seuil sera également facilement atteint avec les fonctions de haut niveau, auquel cas
l'utilisateur devra envisager de restreindre ses requêtes.
Par ailleurs, la gestion du cache par les fonctions de bas niveau est de la responsabilité
de l'utilisateur.

Dans les deux cas, les fonctions implémentées sont conçues pour boucler sur les résultats de la
requête : les arguments optionnels `size` et `page` ou `cursor` ne doivent pas être fournis
au client python.

## Fonctions de haut niveau

{: .warning }
> Certaines stations ont des paramètres géographiques incomplets (
> `code_region`, `code_departement`, `code_commune`, `code_bassin_dce`,
> `code_sous_bassin` notamment). Il n'est donc pas possible pour cl-hubeau
> d'implémenter des boucles satisfaisantes en utilisant les critères définis par
> l'API.
>
> A la place, les données stations sont requêtées en utilisant une grille
> spatiale basée sur l'enveloppe des territoires présents dans les jeux de
> données AdminExpress de l'IGN. Cette grille est ensuite utilisée pour boucler
> sur les "boîtes" (usage du paramètre `bbox`).
>
> Un post-traitement est ensuite utilisé pour compléter les champs manquants
> dans le dataframe retourné. Cette consolidation est effectuée en deux étapes :
>
> * en premier lieu une jointure spatiale
> * en second lieu une jointure spatiale approximative (dans une limite de 10km)
>
> Ces données consolidées sont ensuite utilisées pour le requêtage de chaque
> endpoint de l'API *Qualité des Eaux Superficielles*. **Aucune consolidation
> n'est effectuée sur les autres jeux de données** : l'utilisateur est à la place
> invité à créer les liaisons pertinentes avec le jeu de données des stations.
>
> Par la suite, toutes les données datées seront requêtées par tranches de
> 6 mois calendaires pour optimiser le cache ; dans le cas d'atteinte du
> seuil de 20k résultats, les périodes seront automatiquement divisées par deux
> pour garantir l'obtention des résultats.

{: .critical }
Par conséquent, les données des territoires non représentés dans le jeux de
données AdminExpress (certains territoires d'outre-mer) ne peuvent pas être
récupérée à ce jour.

{: .critical }
> Afin de limiter le temps de calcul lié à la consolidation des résultats, tous
> les champs ne sont pas comblés. A ce jour, seules les clefs de requêtage les
> plus courantes ont été implémentées :
>
> * données du code officiel géographique (région, département, commune)
> * bassins DCE et sous-bassins
>
> Dans le cas où vous souhaiteriez voir ajouter une couche de consolidation,
> merci de créer une issue sur le repo, si possible en sourçant un jeu de
> données national APIsé ad hoc.


### Récupération de la totalité des stations

Cette fonction permet de récupérer les stations de mesures physicochimique en
cours d'eau de la France entière.

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux
supportés par le point de sortie "station_pc" de l'API.

Par exemple :
```python
from cl_hubeau import superficial_waterbodies_quality
gdf = superficial_waterbodies_quality.get_all_stations(code_region="32")
```

### Récupération des opérations

Cette fonction permet de récupérer les opérations physicochimiques sur des cours
d'eau et plan d'eau en France métropolitaine et DROM.

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_operations()
```

{: .warning }
Ce type de requêtage induit rapidement des résultats volumineux.
S'il est en théorie possible de requêter l'API sans paramétrage via cette
fonction, il est fortement conseillé d'utiliser des arguments suplémentaires
pour restreindre les résultats.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "operation_pc" de l'API.

Par exemple :

```python
from cl_hubeau import superficial_waterbodies_quality
gdf = superficial_waterbodies_quality.get_all_operations(code_region=['32'])
```

### Récupération des conditions environnementales

Cette fonction permet de récupérer les conditions environnementales
physicochimiques d'analyses sur des cours d'eau et plan d'eau
en France métropolitaine et DROM.

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_environmental_conditions()
```

{: .warning }
Ce type de requêtage induit rapidement des résultats volumineux.
S'il est en théorie possible de requêter l'API sans paramétrage via cette
fonction, il est fortement conseillé d'utiliser des arguments suplémentaires
pour restreindre les résultats.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux
supportés par le point de sortie "condition_environnementale_pc" de l'API.

Par exemple :

```python
from cl_hubeau import superficial_waterbodies_quality
gdf = superficial_waterbodies_quality.get_all_environmental_conditions(code_departement=['59'])
```

### Récupération des analyses physico-chimiques

Cette fonction permet de récupérer les analyses physico-chimiques
physicochimiques d'analyses sur des cours d'eau et plan d'eau
en France métropolitaine et DROM.

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_analyses()
```

{: .critical }
> Ce type de requêtage induit des résultats volumineux. Cette API ne **peut pas**
> être requêtée sans paramétrage supplémentaire (le résultat ne tenant pas en
> mémoire sur des machines usuelles).
> Il est de la responsabilité de l'utilisateur d'optimiser le requêtage par
> zone géographique (code département, etc.) et en ciblant certaines substances.
> Il est par ailleurs conseillé de basculer
> sur un format json avec seuls les champs utiles.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux
supportés par le point de sortie "analyse_pc" de l'API.

Par exemple :

```python
from cl_hubeau import superficial_waterbodies_quality
gdf = superficial_waterbodies_quality.get_all_analyses(
    code_departement="59",
    code_parametre="1313",
    format="json",
    fields=[
        "code_station",
        "code_parametre",
        "code_fraction",
        "code_analyse",
        "code_qualification",
        "code_statut",
        "date_prelevement",
        "heure_prelevement",
        "resultat",
        "incertitude_analytique",
        "limite_detection",
        "symbole_unite",
    ]
)
```

## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les stations

```python
from cl_hubeau import superficial_waterbodies_quality
with superficial_waterbodies_quality.SuperficialWaterbodiesQualitySession() as session:
    df = session.get_stations(nom_commune="LILLE")
```

### Lister les opérations

```python
from cl_hubeau import superficial_waterbodies_quality
with cl_hubeau.superficial_waterbodies_quality.SuperficialWaterbodiesQualitySession() as session:
    df = session.get_operations(
        code_departement='02',
        code_parametre="1340",
        date_debut_prelevement="2023-01-01",
        )
```

### Lister les conditions environnementales

```python
from cl_hubeau import superficial_waterbodies_quality
with superficial_waterbodies_quality.SuperficialWaterbodiesQualitySession() as session:
    df = session.get_environmental_conditions(
        code_departement='59',
        date_debut_prelevement="2023-01-01",
        format="geojson",
        )
```

### Lister les analyses physico-chimiques

```python
from cl_hubeau import superficial_waterbodies_quality
with superficial_waterbodies_quality.SuperficialWaterbodiesQualitySession() as session:
    df = session.get_analyses(
        code_departement='02',
        code_parametre="1313",
        date_debut_prelevement="2024-01-01",
        format="geojson",
        )
```
