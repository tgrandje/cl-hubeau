# -*- coding: utf-8 -*-


def pytest_sessionstart(session):
    from cl_hubeau.utils import clean_all_cache

    clean_all_cache()


def pytest_sessionfinish(session, exitstatus):
    from cl_hubeau.utils import clean_all_cache

    clean_all_cache()
