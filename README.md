# cl-hubeau

Simple hub'eau client for python

## Basic examples

### Piezometry

```python

from cl_hubeau import PiezometrySession

with PiezometrySession() as session:
    df = session.get_chronicles(code_bss="07548X0009/F")
    df = session.get_stations(code_departement=['02', '59', '60', '62', '80'], format="geojson")
    df = session.get_chronicles_real_time(code_bss="07548X0009/F")

```