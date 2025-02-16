---
layout: default
title: API Qualité de l'eau potable
language: fr
handle: /drinking-water-quality
nav_order: 8

---
# API Qualité de l'eau potable

[https://hubeau.eaufrance.fr/page/api-qualite-eau-potable](https://hubeau.eaufrance.fr/page/api-qualite-eau-potable)

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

### Récupération de la totalité des unités de distribution

Cette fonction permet de récupérer les unités de distribution (UDI) de la France entière.

```python
from cl_hubeau import drinking_water_quality
df = drinking_water_quality.get_all_water_networks()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "UDI" de l'API, à l'exception de :
* `code_commune` (utilisé pour boucler sur les codes communes)

Par exemple :
```python
from cl_hubeau import drinking_water_quality
gdf = drinking_water_quality.get_all_water_networks(annee=2024)
```

### Récupération des résultats du contrôle sanitaire

Cette fonction permet de récupérer les chroniques de données temps différé pour une liste de piézomètres.
Ceux-ci doivent être spécifiés sous la forme d'une liste de codes bss (seul champ disponible pour
identifier un piézomètre sur ce point de sortie API).

```python
from cl_hubeau import drinking_water_quality
df = drinking_water_quality.get_control_results(
    codes_reseaux=['013000519', '013000521'],
    code_parametre="1340"
)
```

{: .warning }
Ce type de requêtage induit rapidement des résultats volumineux, même en ciblant une seule substance.
En théorie, il est possible de requêter l'API sans spécifier la substance ciblée, mais
la probabilité d'atteindre le seuil maximal de 20 000 résultats est élevé.

En l'état, cette fonction implémente déjà une double boucle :
* sur les codes UDI
* sur les périodes, en requêtant par plage de 6 mois calendaires (ce qui permet
une scalabilité de l'algorithme dans le temps et optimise l'usage du cache).

Il est également possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "chroniques" de l'API, à l'exception de `code_reseau`.

Par exemple :
```python
from cl_hubeau import drinking_water_quality
df = drinking_water_quality.get_control_results(
    codes_communes=['59350'],
    code_parametre="1340"
    )
```

## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les communes et les UDI (réseaux)

```python
from cl_hubeau import drinking_water_quality
with drinking_water_quality.DrinkingWaterQualitySession() as session:
    df = session.get_cities_networks(nom_commune="LILLE")
```

### Lister les analyses d'une ou plusieurs UDI

```python
from cl_hubeau import drinking_water_quality
with drinking_water_quality.DrinkingWaterQualitySession() as session:
    df = session.get_control_results(code_departement='02', code_parametre="1340")
```
