---
layout: default
title: Utilitaires annexes
language: fr
handle: /
nav_order: 20

---

# Utilitaires

Pour faciliter le requêtage sur des territoires hydrographiques cohérents, certains
utilitaires ont été ajoutés au module.

Dans ce process, des jeux de données officiels sont moissonnés (ces jeux de données ne
figurant pas sur hub'eau). Dans un second temps, de simples jointures spatiales sont effectuées
avec le dernier millésime des couches communales.

Ces fonctions sont de **simples utilitaires** et elles sont sujettes à des **effets de bord** 
(la précision géographique des deux datasets ne sera probablement pas identique).

## SAGE (Schéma d'Aménagement et de Gestion des Eaux)

Il est possible de récupérer la composition communale des SAGE en utilisant le code suivant :

```python
from cl_hubeau.utils import cities_for_sage

d = cities_for_sage()
```

Le jeu de données utilisé est la couche SANDRE du catalogue eaufrance.

