#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 15:27:18 2024

@author: thomasgrandjean
"""

import cl_hubeau
from cl_hubeau import hydrometry
from cl_hubeau.utils import clean_all_cache

clean_all_cache()
cl_hubeau._config["THREADS"] = 1

code = "U3434340"
with hydrometry.HydrometrySession() as session:
    df = session.get_realtime_observations(code_entite=code)

print(df)
