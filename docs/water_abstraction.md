---
layout: default
title: API Prélèvements en eau
language: fr
handle: /water-abstraction
nav_order: 12

---
# API Prélèvements en eau

[https://hubeau.eaufrance.fr/page/api-prelevements-eau](https://hubeau.eaufrance.fr/page/api-prelevements-eau)

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
> Certains ouvrages ont des paramètres géographiques incomplets (
> `code_region`, `code_departement`, `code_commune`, `code_bassin_dce`,
> `code_sous_bassin` notamment). Il n'est donc pas possible pour cl-hubeau
> d'implémenter des boucles satisfaisantes en utilisant les critères définis par
> l'API.
>
> A la place, les données **ouvrages** sont requêtées en utilisant une grille
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
> endpoint de l'API *Température des cours d'eau* (en itérant sur les stations
* ainsi consolidée). **Aucune consolidation
> n'est effectuée sur les autres jeux de données** : l'utilisateur est à la place
> invité à créer les liaisons pertinentes avec le jeu de données des stations.

{: .critical }
Par conséquent, les données des territoires non représentés dans le jeux de
données AdminExpress (certains territoires d'outre-mer) ne peuvent pas être
récupérées à ce jour.

{: .critical }
> Afin de limiter le temps de calcul lié à la consolidation des résultats, tous
> les champs ne sont pas comblés. A ce jour, seules les clefs de requêtage les
> plus courantes ont été implémentées :
>
> * données du code officiel géographique (département, commune)
>
> Dans le cas où vous souhaiteriez voir ajouter une couche de consolidation,
> merci de créer une issue sur le repo, si possible en sourçant un jeu de
> données national APIsé ad hoc.


### Récupération de la totalité des ouvrages

Cette fonction permet de récupérer ouvrages de la France entière.

```python
from cl_hubeau import water_abstraction
df = water_abstraction.get_all_ouvrages()
```

Il est également possible de spécifier des arguments à la fonction, parmi ceux
supportés par le point de sortie "ouvrage" de l'API.

Par exemple :
```python
from cl_hubeau import water_abstraction
gdf = water_abstraction.get_all_ouvrages(code_departement="59")
```

### Récupération des points de prélévements

Cette fonction permet de récupérer les points de prélévements en France métropolitaine et DROM.

```python
from cl_hubeau import water_abstraction
df = water_abstraction.get_all_points_prelevement()
```

{: .warning }
Ce type de requêtage induit des résultats volumineux.
S'il est en théorie possible de requêter l'API sans paramétrage via cette
fonction, il est fortement conseillé d'utiliser des arguments supplémentaires
pour restreindre les résultats.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "points_prelevement" de l'API.

Par exemple :

```python
from cl_hubeau import water_abstraction
gdf = water_abstraction.get_all_points_prelevement(
  code_departement=['59'],
  )
```

### Récupération des chroniques de mesures

Cette fonction permet de récupérer les mesures de prélévements en France métropolitaine et DROM.

```python
from cl_hubeau import water_abstraction
df = water_abstraction.get_all_chronicles()
```

{: .warning }
Ce type de requêtage induit des résultats volumineux.
S'il est en théorie possible de requêter l'API sans paramétrage via cette
fonction, il est fortement conseillé d'utiliser des arguments supplémentaires
pour restreindre les résultats.

Il est ainsi possible de spécifier des arguments à la fonction, parmi ceux supportés
par le point de sortie "chronicles" de l'API.

Par exemple :

```python
from cl_hubeau import water_abstraction
gdf = water_abstraction.get_all_chronicles(
  code_departement=['59'],
  )
```

## Fonctions de bas niveau

Un objet session est défini pour consommer l'API à l'aide de méthodes de bas niveau.
Ces méthodes correspondent strictement aux fonctions disponibles via l'API : l'utilisateur
est invité à se reporter à la documentation de l'API concernant le détail des arguments
disponibles.

### Lister les ouvrages

```python
from cl_hubeau import water_abstraction
with water_abstraction.AbstractionSession() as session:
    df = session.get_ouvrages(libelle_commune="MORCOURT", format="geojson")
```

### Lister les points de prélévement

```python
from cl_hubeau import water_abstraction
with water_abstraction.AbstractionSession() as session:
    df = session.get_points_prelevement(
        code_departement='02',
        )
```

### Lister les chroniques

```python
from cl_hubeau import water_abstraction
with water_abstraction.AbstractionSession() as session:
    df = session.get_chronicles(
        code_departement='02',
        )
```
