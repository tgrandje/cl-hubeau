---
layout: default
title: Trucs et astuces
language: fr
handle: /good-practices
nav_order: 6

---
# Optimiser la RAM

Certaines requêtes sont lourdes : des gains peuvent être (souvent) obtenus en sélectionnant les champs recherchés (critère `fields`)
et en évitant le format `"geojson"` qui induit nécessairement la récupération des géométries.

En tout état de cause, il est particulièrement pertinent de tirer parti des liaisons relationnelles
entre les différents endpoint d'une même API. Par exemple, les géométries peuvent souvent
être récupérées via les stations et une jointure faite avec les observations.
De cette manière, on fait l'économie de la démultiplication des géométries au travers
des observations.

# Accélérer les résultats

Lorsque les opérations de haut niveau utilisent un comblement des données, il peut parfois
être pertinent de désactiver ce comblement pour obtenir des données brutes plus rapidement.
Bien évidemment, il est alors de la responsabilité de l'utilisateur de s'assurer de la
complétude des champs recherchés ou interrogés.
