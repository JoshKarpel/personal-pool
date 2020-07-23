#!/usr/bin/env python3

import logging
from pathlib import Path
import shutil

from personal import PersonalPool

logging.basicConfig(format="%(asctime)s %(name)s ~ %(msg)s", level=logging.DEBUG)


# def print_ad(ad):
#     print(ad["ClusterId"], ad["ProcId"], ad["JobStatus"], ad["Cmd"], ad["Args"])
#
#
# path = Path.cwd() / "test-condor"
# shutil.rmtree(path, ignore_errors=True)
#
# p = PersonalPool().start()
#
# p2 = PersonalPool().attach()
#
# print(p)
# print(p2)
#
# # p.stop()
#
# p2.stop()

with PersonalPool(use_config=False).start() as pool:
    print(pool.get_config_val("FOOBAR"))
