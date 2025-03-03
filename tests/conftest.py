# -*- coding: utf-8 -*-

from filelock import FileLock


def pytest_sessionstart(session):

    from cl_hubeau.utils import clean_all_cache
    from cl_hubeau.session import BaseHubeauSession

    with FileLock("test.lock", thread_local=False):
        with BaseHubeauSession() as s:
            if s.cache.responses == []:
                # new virgin cache -> those are nth+1 workers loaded by
                # pytest-xdist
                return

        # this is the first worker passing
        clean_all_cache()
        with BaseHubeauSession():
            # create the new session object
            pass
