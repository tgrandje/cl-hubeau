---
layout: default
title: Parallélisation
language: fr
handle: /
nav_order: 3

---

# Parallélisation

`cl-hubeau` utilise déjà du multithreading pour obtenir des résultats plus
rapides.
Afin de ne pas mettre en péril les serveurs web et de partager les ressources 
équitablement entre les utilisateurs, un limiteur de débit est réglé sur 
10 requêtes par seconde. 

Ce limiteur devrait fonctionner correctement sur n'importe quelle machine, quel
que soit le contexte (même avec une couche supplémentaire de parallélisation).

{: .critical }
Cependant `cl-hubeau` ne doit  **PAS** être utilisé sur des clusters de
machines (conteneurs ou pods) incluant une couche de parallélisation. 
Il n'existe à ce jour aucun moyen permettant de suivre le taux de requêtes
sur plusieurs machines : un usage immodéré risque d'entraîner l'inscription sur
liste noire par les équipes.
