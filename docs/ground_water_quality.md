---
layout: default
title: API Qualité des nappes
language: fr
handle: /ground-water-quality
nav_order: 8

---
# API Qualité des nappes

[https://hubeau.eaufrance.fr/page/api-qualite-nappes](https://hubeau.eaufrance.fr/page/api-qualite-nappes)

`cl-hubeau` définit :

* des fonctions de haut niveau implémentant des boucles basiques ;
* des fonctions de bas niveau qui implémentent directement les différents points d'entrée de l'API.

{: .warning }
Lors de l'utilisation des fonctions de bas niveau, l'utilisateur est responsable
de la consommation de l'API. En particulier, il s'agit d'être vigilant quant au seuil
de 20 000 résultats récupérables d'une seule requête.
Par ailleurs, la gestion du cache par les fonctions de bas niveau est de la responsabilité
de l'utilisateur.

Dans les deux cas, les fonctions implémentées sont conçues pour boucler sur les résultats de la
requête : les arguments optionnels `size` et `page` ou `cursor` ne doivent pas être fournis
au client python.

## Fonctions de haut niveau

### Récupération de la totalité des stations de mesure

Cette fonction permet de récupérer les stations de mesure sur la France entière.

```python
from cl_hubeau import ground_water_quality
df = ground_water_quality.get_all_stations()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie de l'API.

Par exemple :
```python
from cl_hubeau import ground_water_quality
gdf = ground_water_quality.get_all_stations(
    bassin_dce="L'Escaut, la Somme et les cours d'eau côtiers de la Manche et de la Mer du Nord"
    )
```

### Récupération des résultats des analyses

Cette fonction permet de récupérer les chroniques de données temps différé pour une liste de piézomètres.
Ceux-ci doivent être spécifiés sous la forme d'une liste de codes bss (seul champ disponible pour
identifier un piézomètre sur ce point de sortie API).

```python
from cl_hubeau import ground_water_quality
df = ground_water_quality.get_all_analyses(
    code_param="1340"
)
```

{: .warning }
Ce type de requêtage induit rapidement des résultats volumineux.
En théorie, il est possible de requêter l'API sans spécifier la substance ciblée ni les
qualitomètres, mais  la probabilité d'atteindre le seuil maximal de 20 000 résultats est élevé.

En l'état, cette fonction implémente déjà une double boucle :
* sur les codes départements
* sur les périodes, en requêtant par plage de 6 mois (ce qui permet une scalabilité de l'algorithme dans le temps
et optimise l'usage du cache).

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "chroniques" de l'API.

Par exemple :
```python
from cl_hubeau import ground_water_quality
df = ground_water_quality.get_control_results(
    bss_id=['BSS000EFUA', 'BSS000EWPC'],
    code_param="1340"
    )
```

## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les stations

```python
from cl_hubeau import ground_water_quality
with ground_water_quality.GroundWaterQualitySession() as session:
    df = session.get_stations(num_departement="59")
```

### Lister les analyses

```python
from cl_hubeau import ground_water_quality
with ground_water_quality.GroundWaterQualitySession() as session:
    df = session.get_analyses(bss_id='BSS000EFUA', code_param="1340")
```
