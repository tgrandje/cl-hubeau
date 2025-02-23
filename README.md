# cl-hubeau

![PyPI - Version](https://img.shields.io/pypi/v/cl-hubeau)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/cl-hubeau)](https://pypi.python.org/pypi/cl-hubeau/)
![PyPI - Status](https://img.shields.io/pypi/status/cl-hubeau)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![flake8 checks](https://raw.githubusercontent.com/tgrandje/cl-hubeau/refs/heads/main/badges/flake8-badge.svg)
![Test Coverage](https://raw.githubusercontent.com/tgrandje/cl-hubeau/refs/heads/main/badges/coverage-badge.svg)
![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/tgrandje/cl-hubeau)
![GitHub commits since latest release](https://img.shields.io/github/commits-since/tgrandje/cl-hubeau/latest)

![Monthly Downloads](https://img.shields.io/pypi/dm/cl-hubeau)
![Total Downloads](https://img.shields.io/pepy/dt/cl-hubeau)

![Hub'eau Coverage](https://raw.githubusercontent.com/tgrandje/cl-hubeau/refs/heads/main/badges/hubeau-coverage.svg)

Simple hub'eau client for python

This package is currently under active development.
Every API on [Hub'eau](hubeau.eaufrance.fr/) will be covered by this package in
due time.

At this stage, the following APIs are covered by cl-hubeau:
* [piezometry/piézométrie](https://hubeau.eaufrance.fr/page/api-piezometrie)
* [hydrometry/hydrométrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)
* [drinking water quality/qualité de l'eau potable](https://hubeau.eaufrance.fr/page/api-qualite-eau-potable)
* [superficial waterbodies quality/qualité physico-chimique des cours d'eau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau)
* [ground waterbodies quality/qualité des nappes](https://hubeau.eaufrance.fr/page/api-qualite-nappes)
* [watercourses flow/écoulement des cours d'eau](https://hubeau.eaufrance.fr/page/api-ecoulement)


For any help on available kwargs for each endpoint, please refer
directly to the documentation on hubeau (this will not be covered
by the current documentation).

Assume that each function from cl-hubeau will be consistent with
it's hub'eau counterpart, with the exception of the `size` and
`page` or `cursor` arguments (those will be set automatically by
cl-hubeau to crawl allong the results).

## Parallelization

`cl-hubeau` already uses simple multithreading pools to perform requests.
In order not to endanger the webservers and share ressources among users, a
rate limiter is set to 10 queries per second. This limiter should work fine on
any given machine, whatever the context (even with a new parallelization
overlay).

However `cl-hubeau` should **NOT** be used in containers (or pods) with
parallelization. There is currently no way of tracking the queries' rate
among multiple machines: greedy queries may end up blacklisted by the
team managing Hub'eau.


## Configuration

Starting with `pynsee 0.2.0`, no API keys are needed anymore.

## Support

In case of bugs, please open an issue [on the repo](https://github.com/tgrandje/cl-hubeau/issues).

## Contribution
Any help is welcome. Please refer to the [CONTRIBUTING file](https://github.com/tgrandje/cl-hubeau/CONTRIBUTING.md).

## Licence
GPL-3.0-or-later

## Project Status

This package is currently under active development.

## Basic examples

### Clean cache

```python
from cl_hubeau.utils import clean_all_cache
clean_all_cache
```

### Piezometry

3 high level functions are available (and one class for low level operations).

Get all piezometers (uses a 30 days caching):

```python
from cl_hubeau import piezometry
gdf = piezometry.get_all_stations()
```

Get chronicles for the first 100 piezometers (uses a 30 days caching):

```python
df = piezometry.get_chronicles(gdf["code_bss"].head(100).tolist())
```

Get realtime data for the first 100 piezometers:

A small cache is stored to allow for realtime consumption (cache expires after
only 15 minutes). Please, adopt a responsible usage with this functionnality !

```python
df = get_realtime_chronicles(gdf["code_bss"].head(100).tolist())
```

Low level class to perform the same tasks:

Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility, noticely for realtime data

```python
with piezometry.PiezometrySession() as session:
    df = session.get_chronicles(code_bss="07548X0009/F")
    df = session.get_stations(code_departement=['02', '59', '60', '62', '80'], format="geojson")
    df = session.get_chronicles_real_time(code_bss="07548X0009/F")
```

### Hydrometry

4 high level functions are available (and one class for low level operations).


Get all stations (uses a 30 days caching):

```python
from cl_hubeau import hydrometry
gdf = hydrometry.get_all_stations()
```

Get all sites (uses a 30 days caching):

```python
gdf = hydrometry.get_all_sites()
```

Get observations for the first 5 sites (uses a 30 days caching):
_Note that this will also work with stations (instead of sites)._

```python
df = hydrometry.get_observations(gdf["code_site"].head(5).tolist())
```

Get realtime data for the first 5 sites (no cache stored):

A small cache is stored to allow for realtime consumption (cache expires after
only 15 minutes). Please, adopt a responsible usage with this functionnality !


```python
df = hydrometry.get_realtime_observations(gdf["code_site"].head(5).tolist())
```

Low level class to perform the same tasks:


Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility, noticely for realtime data

```python
with hydrometry.HydrometrySession() as session:
    df = session.get_stations(code_station="K437311001")
    df = session.get_sites(code_departement=['02', '59', '60', '62', '80'], format="geojson")
    df = session.get_realtime_observations(code_entite="K437311001")
    df = session.get_observations(code_entite="K437311001")

```

### Drinking water quality

2 high level functions are available (and one class for low level operations).


Get all water networks (UDI) (uses a 30 days caching):

```python
from cl_hubeau import drinking_water_quality
df = drinking_water_quality.get_all_water_networks()
```

Get the sanitary controls's results for nitrates on all networks of Paris, Lyon & Marseille
(uses a 30 days caching) for nitrates

```python
networks = drinking_water_quality.get_all_water_networks()
networks = networks[
    networks.nom_commune.isin(["PARIS", "MARSEILLE", "LYON"])
    ]["code_reseau"].unique().tolist()

df = drinking_water_quality.get_control_results(
    codes_reseaux=networks,
    code_parametre="1340"
)
```

Note that this query is heavy, even if this was already restricted to nitrates.
In theory, you could also query the API without specifying the substance you're tracking,
but you may hit the 20k threshold and trigger an exception.

You can also call the same function, using official city codes directly:
```python
df = drinking_water_quality.get_control_results(
    codes_communes=['59350'],
    code_parametre="1340"
)
```

Low level class to perform the same tasks:


Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility

```python
with drinking_water_quality.DrinkingWaterQualitySession() as session:
    df = session.get_cities_networks(nom_commune="LILLE")
    df = session.get_control_results(code_departement='02', code_parametre="1340")

```

### Superficial waterbodies quality

4 high level functions are available (and one class for low level operations).


Get all stations (uses a 30 days caching):

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_stations()
```

Get all operations (uses a 30 days caching):

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_operations()
```

Note that this query is heavy, users should restrict it to a given territory.
For instance, you could use :
```python
df = superficial_waterbodies_quality.get_all_operations(code_region="11")
```

Get all environmental conditions:

```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_environmental_conditions()
```

Note that this query is heavy, users should restrict it to a given territory.
For instance, you could use :
```python
df = superficial_waterbodies_quality.get_all_environmental_conditions(code_region="11")
```

Get all physicochemical analyses:
```python
from cl_hubeau import superficial_waterbodies_quality
df = superficial_waterbodies_quality.get_all_analyses()
```

Note that this query is heavy, users should restrict it to a given territory
and given parameters. For instance, you could use :
```python
df = superficial_waterbodies_quality.get_all_analyses(
    code_departement="59",
    code_parametre="1313"
    )
```


Low level class to perform the same tasks:


Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility

```python
with superficial_waterbodies_quality.SuperficialWaterbodiesQualitySession() as session:
    df = session.get_stations(code_commune="59183")
    df = session.get_operations(code_commune="59183")
    df = session.get_environmental_conditions(code_commune="59183")
    df = session.get_analyses(code_commune='59183', code_parametre="1340")

```

### Ground water quality

2 high level functions are available (and one class for low level operations).


Get all stations (uses a 30 days caching):

```python
from cl_hubeau import ground_water_quality
df = ground_water_quality.get_all_stations()
```

Get the tests results for nitrates :

```python
df = ground_water_quality.df = get_all_analyses(code_param="1340")
```

Note that this query is heavy, even if this was already restricted to nitrates, and that it
may fail. In theory, you could even query the API without specifying the substance
you're tracking, but you will hit the 20k threshold and trigger an exception.

In practice, you should call the same function with a territorial restriction or with
specific bss_ids.
For instance, you could use official city codes directly:

```python
df = ground_water_quality.get_all_analyses(
    num_departement=["59"]
    code_param="1340"
)
```

Note: a bit of caution is needed here, as the arguments are **NOT** the same
in the two endpoints. Please have a look at the documentation on
[hubeau](https://hubeau.eaufrance.fr/page/api-qualite-nappes#/qualite-nappes/analyses).
For instance, the city's number is called `"code_insee_actuel"` on analyses' endpoint
and `"code_commune"` on station's.

Low level class to perform the same tasks:


Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility

```python
with ground_water_quality.GroundWaterQualitySession() as session:
    df = session.get_stations(bss_id="01832B0600")
    df = session.get_analyses(
        bss_ids=["BSS000BMMA"],
        code_param="1461",
        )
```

### Watercourses flow

3 high level functions are available (and one class for low level operations).



get_all_campaigns

Get all stations (uses a 30 days caching):

```python
from cl_hubeau import watercourses_flow
df = watercourses_flow.get_all_stations()
```

Get all observations (uses a 30 days caching):

```python
from cl_hubeau import watercourses_flow
df = watercourses_flow.get_all_observations()
```

Note that this query is heavy, users should restrict it to a given territory when possible.
For instance, you could use :
```python
df = watercourses_flow.get_all_observations(code_region="11")
```

Get all campagins:

```python
from cl_hubeau import watercourses_flow
df = watercourses_flow.get_all_campaigns()
```

Low level class to perform the same tasks:


Note that :

* the API is forbidding results > 20k rows and you may need inner loops
* the cache handling will be your responsibility

```python
with watercourses_flow.WatercoursesFlowSession() as session:
    df = session.get_stations(code_departement="59")
    df = session.get_campaigns(code_campagne=[12])
    df = session.get_observations(code_station="F6640008")

```
