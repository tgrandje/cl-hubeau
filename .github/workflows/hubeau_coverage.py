#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
utilitary script used to generate hub'eau coverage badge
"""
from glob import glob
import os

from lxml import html
import requests
from genbadge import Badge


def get_api_count():
    url = "https://hubeau.eaufrance.fr/page/apis"
    r = requests.get(url)
    etree = html.fromstring(r.content)
    apis = etree.xpath(".//a[@class='api-card']")

    print(f"{len(apis)} apis presently available on hubeau")

    return len(apis)


def get_localdirs():
    dirs = [
        x
        for x in glob("cl_hubeau/*/")
        if os.path.isdir(x)
        and not any(y in x for y in {"session", "utils", "pycache"})
    ]
    print(f"{len(dirs)} api covered on the current branch:")
    print("\n".join(dirs))
    return len(dirs)


def generate_badge(local_coverage: int, api_count: int):
    b = Badge(
        left_txt="couverture hub’eau",
        right_txt=f"{local_coverage} API / {api_count}",
        color="green",
    )

    print(f"generate badge with stat '{local_coverage} API / {api_count}'")

    b.write_to("./badges/hubeau-coverage.svg", use_shields=False)
    print("badge written on ./badges/hubeau-coverage.svg")


if __name__ == "__main__":
    api_count = get_api_count()
    local_coverage = get_localdirs()
    generate_badge(local_coverage, api_count)
