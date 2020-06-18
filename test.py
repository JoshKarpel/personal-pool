#!/usr/bin/env python3

import logging
from pathlib import Path
import shutil

from personal import PersonalPool

logging.basicConfig(format="%(asctime)s %(name)s ~ %(msg)s", level=logging.DEBUG)


def print_ad(ad):
    print(ad["ClusterId"], ad["ProcId"], ad["JobStatus"], ad["Cmd"], ad["Args"])


path = Path.cwd() / "test-condor"
shutil.rmtree(path, ignore_errors=True)

with PersonalPool(path, config={"foo": "bar"}) as pool:
    print(
        pool.run_command(
            ["condor_config_val", "-dump", "ALLOW", "UID_DOMAIN", "self"]
        ).stdout
    )
    print(pool.run_command(["condor_status"]).stdout)
    print(pool.run_command(["condor_submit", "test.sub"]).stdout)
    print(pool.run_command(["condor_q"]).stdout)
    print(pool.get_config_val("LOCAL_DIR"))

    off = pool.run_command(["condor_off"])
    print(off.stdout)
    print(off.stderr)
