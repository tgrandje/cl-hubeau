# cl-hubeau

Simple hub'eau client for python

This package is currently under active development.
Every API on [Hub'eau](hubeau.eaufrance.fr/) will be covered by this package in
due time.

At this stage, the following APIs are covered by cl-hubeau:
* [piezometry/piézométrie](https://hubeau.eaufrance.fr/page/api-piezometrie)
* [hydrometry/hydrométrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)
* [drinking water quality/qualité de l'eau potable](https://hubeau.eaufrance.fr/page/api-qualite-eau-potable#/qualite_eau_potable/communes)

For any help on available kwargs for each endpoint, please refer 
directly to the documentation on hubeau (this will not be covered
by the current documentation).

Assume that each function from cl-hubeau will be consistent with
it's hub'eau counterpart, with the exception of the `size` and 
`page` or `cursor` arguments (those will be set automatically by
cl-hubeau to crawl allong the results).

## Configuration

First of all, you will need API keys from INSEE to use some high level operations, 
which may loop over cities'official codes. Please refer to pynsee's
[API subscription Tutorial ](https://pynsee.readthedocs.io/en/latest/api_subscription.html)
for help.

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

As it is, the `get_control_results` function already implements a double loop:

* on networks' codes (20 codes maximum) ;
* on periods, requesting only yearly datasets (which should be scalable over time **and** should work nicely with the cache algorithm).

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