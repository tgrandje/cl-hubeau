---
layout: default
title: API Qualité des cours d'eau
language: fr
handle: /drinking-water-quality
nav_order: 9

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

### Récupération de la totalité des stations

Cette fonction permet de récupérer les stations de mesures physicochimique en cours d'eau de la France entière.

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "station_pc" de l'API, à l'exception de :
* `code_departement` (utilisé pour boucler sur les codes départements)

Par exemple :
```python
from cl_hubeau import superficial_waterbodies_quality
gdf = superficial_waterbodies_quality.get_all_stations(code_region="32")
```

### Récupération des opérations

Cette fonction permet de récupérer les opérations physicochimiques sur des cours d'eau et plan d'eau
en France métropolitaine et DROM.

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_operations()
```

{: .warning }
Ce type de requêtage induit rapidement des résultats volumineux.
S'il est en théorie possible de requêter l'API sans paramétrage via cette fonction, il est fortement
conseillé d'utiliser des arguments suplémentaires pour restreindre les résultats.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "operation_pc" de l'API.

En l'état, cette fonction implémente déjà des boucles :
* sur les codes départementaux (y compris en cas de recherche par code région) ;
* sur les périodes, en requêtant par tranche de 6 mois (ce qui permet une scalabilité de l'algorithme dans le temps
et optimise l'usage du cache).

Par exemple :

```python
from cl_hubeau import superficial_waterbodies_quality
gdf = superficial_waterbodies_quality.get_all_stations(code_region=['32'])
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
S'il est en théorie possible de requêter l'API sans paramétrage via cette fonction, il est fortement
conseillé d'utiliser des arguments suplémentaires pour restreindre les résultats.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "condition_environnementale_pc" de l'API.

En l'état, cette fonction implémente déjà des boucles :
* sur les codes départementaux (y compris en cas de recherche par code région) ;
* sur les périodes, en requêtant par tranche de 6 mois (ce qui permet une scalabilité de l'algorithme dans le temps
et optimise l'usage du cache).

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

{: .warning }
Ce type de requêtage induit des résultats volumineux. Cette API ne **peut pas** être
requêtée sans paramétrage supplémentaire (dépassement du nombre de 20 000 résultats).
Il est de la responsabilité de l'utilisateur d'optimiser le requêtage par
zone géographique (code département, etc.) et/ou en ciblant certaines substances.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "analyse_pc" de l'API.

En l'état, cette fonction implémente déjà une boucle sur les périodes, en requêtant par
années calendaires (ce qui permet une scalabilité de l'algorithme dans le temps et optimise l'usage du cache).

Par exemple :

```python
from cl_hubeau import superficial_waterbodies_quality
gdf = superficial_waterbodies_quality.get_all_analyses(code_departement="59", code_parametre="1313")
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
