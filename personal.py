# Copyright 2020 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, Mapping, List

import logging
import atexit
import os
import shlex
import subprocess
import functools
import textwrap
import time
from pathlib import Path
import enum
import contextlib

import htcondor
import classad

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "ALL_DEBUG": "D_ALL",
    "TOOL_DEBUG": "D_ALL",
    "LOCAL_CONFIG_FILE": "",
    "MASTER_ADDRESS_FILE": "$(LOG)/.master_address",
    "COLLECTOR_ADDRESS_FILE": "$(LOG)/.collector_address",
    "SCHEDD_ADDRESS_FILE": "$(LOG)/.schedd_address",
    "JOB_QUEUE_LOG": "$(SPOOL)/job_queue.log",
    # TUNING
    "UPDATE_INTERVAL": "2",
    "POLLING_INTERVAL": "2",
    "NEGOTIATOR_INTERVAL": "2",
    "STARTER_UPDATE_INTERVAL": "2",
    "STARTER_INITIAL_UPDATE_INTERVAL": "2",
    "NEGOTIATOR_CYCLE_DELAY": "2",
    "RUNBENCHMARKS": "0",
    "MAX_JOB_QUEUE_LOG_ROTATIONS": "0",
    # SECURITY
    "SEC_DAEMON_AUTHENTICATION": "REQUIRED",
    "SEC_CLIENT_AUTHENTICATION": "REQUIRED",
    "SEC_DEFAULT_AUTHENTICATION_METHODS": "FS, PASSWORD, IDTOKENS",
    "self": "$(USERNAME)@$(UID_DOMAIN) $(USERNAME)@$(IPV4_ADDRESS) $(USERNAME)@$(IPV6_ADDRESS) $(USERNAME)@$(FULL_HOSTNAME) $(USERNAME)@$(HOSTNAME)",
    "ALLOW_READ": "$(self)",
    "ALLOW_WRITE": "$(self)",
    "ALLOW_DAEMON": "$(self)",
    "ALLOW_ADMINISTRATOR": "$(self)",
    "ALLOW_ADVERTISE": "$(self)",
    "ALLOW_NEGOTIATOR": "$(self)",
    "ALLOW_READ_COLLECTOR": "$(ALLOW_READ)",
    "ALLOW_READ_STARTD": "$(ALLOW_READ)",
    "ALLOW_NEGOTIATOR_SCHEDD": "$(ALLOW_NEGOTIATOR)",
    "ALLOW_WRITE_COLLECTOR": "$(ALLOW_WRITE)",
    "ALLOW_WRITE_STARTD": "$(ALLOW_WRITE)",
    # SLOT CONFIG
    "NUM_SLOTS": "1",
    "NUM_SLOTS_TYPE_1": "1",
    "SLOT_TYPE_1": "100%",
    "SLOT_TYPE_1_PARTITIONABLE": "TRUE",
}

ROLES = ["Personal"]
FEATURES = ["GPUs"]

INHERIT = {
    "RELEASE_DIR",
    "LIB",
    "BIN",
    "SBIN",
    "INCLUDE",
    "LIBEXEC",
    "SHARE",
    "AUTH_SSL_SERVER_CAFILE",
    "AUTH_SSL_CLIENT_CAFILE",
    "AUTH_SSL_SERVER_CERTFILE",
    "AUTH_SSL_SERVER_KEYFILE",
}


def skip_if(*states):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.state in states:
                logger.debug(
                    "Skipping call to {} for {} because its state is {}".format(
                        func.__name__, self, self.state
                    )
                )
                return

            return func(self, *args, **kwargs)

        return wrapper

    return decorator


class PersonalPoolState(str, enum.Enum):
    UNINITIALIZED = "uninitialized"
    INITIALIZED = "initialized"
    STARTED = "started"
    READY = "ready"
    STOPPING = "stopping"
    STOPPED = "stopped"


class PersonalPool:
    """
    A :class:`PersonalCondor` is responsible for managing the lifecycle of a
    personal HTCondor pool.
    """

    def __init__(
        self,
        local_dir: Optional[Path] = None,
        config: Mapping[str, str] = None,
        raw_config: str = None,
    ):
        """
        Parameters
        ----------
        local_dir
            The local directory for the HTCondor pool. All HTCondor state will
            be stored in this directory.
        config
            HTCondor configuration parameters to inject, as a mapping of key-value pairs.
        raw_config
            Raw HTCondor configuration language to inject, as a string.
        """
        self._state = PersonalPoolState.UNINITIALIZED
        atexit.register(self._atexit)

        if local_dir is None:
            local_dir = Path.home() / ".condor" / "personal"
        self.local_dir = local_dir.absolute()

        self.execute_dir = self.local_dir / "execute"
        self.lock_dir = self.local_dir / "lock"
        self.log_dir = self.local_dir / "log"
        self.run_dir = self.local_dir / "run"
        self.spool_dir = self.local_dir / "spool"
        self.passwords_dir = self.local_dir / "passwords.d"
        self.tokens_dir = self.local_dir / "tokens.d"

        self.config_file = self.local_dir / "condor_config"

        if config is None:
            config = {}
        self.config = {k: v if v is not None else "" for k, v in config.items()}
        self.raw_config = raw_config or ""

        self.condor_master = None

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        old_state = self._state
        self._state = state
        logger.debug("State of {} changed from {} to {}".format(self, old_state, state))

    def __repr__(self):
        shortest_path = min(
            (
                str(self.local_dir),
                "~/" + str(try_relative_to(self.local_dir, Path.home())),
                "./" + str(try_relative_to(self.local_dir, Path.cwd())),
            ),
            key=len,
        )
        return "{}(local_dir = {}, state = {})".format(
            type(self).__name__, shortest_path, self.state
        )

    def use_config(self):
        """
        Returns a context manager that sets ``CONDOR_CONFIG`` to point to the
        config file for this HTCondor pool.
        """
        return SetCondorConfig(self.config_file)

    @contextlib.contextmanager
    def schedd(self):
        """Yield the :class:`htcondor.Schedd` for this pool's schedd."""
        with self.use_config():
            yield htcondor.Schedd()

    @contextlib.contextmanager
    def collector(self):
        """Yield the :class:`htcondor.Collector` for this pool's collector."""
        with self.use_config():
            yield htcondor.Collector()

    @property
    def master_is_alive(self):
        return self.condor_master is not None and self.condor_master.poll() is None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Stop triggered for {} by context exit.".format(self))
        self.stop()

    def __del__(self):
        logger.debug("Stop triggered for {} by object deletion.".format(self))
        self.stop()

    def _atexit(self):
        logger.debug("Stop triggered for {} by interpreter shutdown.".format(self))
        self.stop()

    def start(self):
        logger.info("Starting {}".format(self))

        try:
            self._initialize()
            self._ready()
        except BaseException:
            logger.exception(
                "Encountered error during setup of {}, cleaning up!".format(self)
            )
            self.stop()
            raise

        logger.info("Started {}".format(self))

        return self

    @skip_if(PersonalPoolState.INITIALIZED)
    def _initialize(self):
        self._setup_local_dirs()
        self._write_config()
        self.state = PersonalPoolState.INITIALIZED

    def _setup_local_dirs(self):
        for dir in (
            self.local_dir,
            self.execute_dir,
            self.lock_dir,
            self.log_dir,
            self.run_dir,
            self.spool_dir,
            self.passwords_dir,
            self.tokens_dir,
        ):
            dir.mkdir(parents=True, exist_ok=True)

    def _write_config(self):
        param_lines = []

        param_lines += ["#", "# INHERITED", "#"]
        for k, v in htcondor.param.items():
            if k not in INHERIT:
                continue
            param_lines += ["{} = {}".format(k, v)]

        param_lines += ["#", "# ROLES", "#"]
        param_lines += ["use ROLE: {}".format(role) for role in ROLES]

        param_lines += ["#", "# FEATURES", "#"]
        param_lines += ["use FEATURE: {}".format(feature) for feature in FEATURES]

        base_config = {
            "LOCAL_DIR": self.local_dir.as_posix(),
            "EXECUTE": self.execute_dir.as_posix(),
            "LOCK": self.lock_dir.as_posix(),
            "LOG": self.log_dir.as_posix(),
            "RUN": self.run_dir.as_posix(),
            "SPOOL": self.spool_dir.as_posix(),
            "SEC_PASSWORD_DIRECTORY": self.passwords_dir.as_posix(),
            "SEC_TOKEN_SYSTEM_DIRECTORY": self.tokens_dir.as_posix(),
        }

        param_lines += ["#", "# BASE PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in base_config.items()]

        param_lines += ["#", "# DEFAULT PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in DEFAULT_CONFIG.items()]

        param_lines += ["#", "# CUSTOM PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in self.config.items()]

        param_lines += ["#", "# RAW PARAMS", "#"]
        param_lines += textwrap.dedent(self.raw_config).splitlines()

        with self.config_file.open(mode="a") as f:
            f.write("\n".join(param_lines))

    def _ready(self):
        self._start_condor()
        self._wait_for_ready()

    @property
    def _has_master(self):
        return self.condor_master is not None

    @skip_if(PersonalPoolState.STARTED)
    def _start_condor(self):
        if self._is_ready():
            raise Exception("Cannot start a second PersonalPool in the same local_dir")

        with SetCondorConfig(self.config_file):
            self.condor_master = subprocess.Popen(
                ["condor_master", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            self.state = PersonalPoolState.STARTED
            logger.debug(
                "Started condor_master (pid {}) for {}".format(
                    self.condor_master.pid, self
                )
            )

    def _daemons(self):
        return set(self.get_config_val("DAEMON_LIST").split(" "))

    @skip_if(PersonalPoolState.READY)
    def _wait_for_ready(self, timeout=120):
        daemons = self._daemons()
        master_log_path = self.master_log

        logger.debug(
            "Starting up daemons for {}, waiting for: {}".format(
                self, " ".join(sorted(daemons))
            )
        )

        start = time.time()
        while time.time() - start < timeout:
            time_to_give_up = int(timeout - (time.time() - start))

            # if the master log does not exist yet, we can't use condor_who
            if not master_log_path.exists():
                logger.debug(
                    "MASTER_LOG at {} does not yet exist for {}, retrying in 1 seconds (giving up in {} seconds).".format(
                        master_log_path, self, time_to_give_up
                    )
                )
                time.sleep(1)
                continue

            who = self.run_command(
                shlex.split(
                    "condor_who -wait:10 'IsReady && STARTD_State =?= \"Ready\"'"
                ),
            )
            if who.stdout.strip() == "":
                logger.debug(
                    "condor_who stdout was unexpectedly blank for {}, retrying in 1 second (giving up in {} seconds).".format(
                        self, time_to_give_up
                    )
                )
                time.sleep(1)
                continue

            who_ad = classad.parseOne(who.stdout)

            if (
                who_ad.get("IsReady")
                and who_ad.get("STARTD_State") == "Ready"
                and all(who_ad.get(d) == "Alive" for d in daemons)
            ):
                self.state = PersonalPoolState.READY
                return

            logger.debug(
                "{} is waiting for daemons to be ready (giving up in {} seconds)".format(
                    self, time_to_give_up
                )
            )

        raise TimeoutError("Standup for {} failed".format(self))

    def _is_ready(self) -> bool:
        who = self.run_command(["condor_who", "-quick"],)
        if who.stdout.strip() == "":
            return False

        who_ad = classad.parseOne(who.stdout)

        return bool(who_ad.get("IsReady"))

    @skip_if(
        PersonalPoolState.STOPPED,
        PersonalPoolState.UNINITIALIZED,
        PersonalPoolState.INITIALIZED,
    )
    def stop(self):
        logger.info("Stopping {}".format(self))

        self.state = PersonalPoolState.STOPPING

        self._condor_off()
        self._wait_for_master_to_terminate()

        self.state = PersonalPoolState.STOPPED

        logger.info("Stopped {}".format(self))

    def _condor_off(self):
        if not self.master_is_alive:
            return

        off = self.run_command(["condor_off", "-daemon", "master"], timeout=30)

        if not off.returncode == 0:
            logger.error(
                "condor_off failed for {}, exit code: {}, stderr: {}".format(
                    self, off.returncode, off.stderr
                )
            )
            self._terminate_condor_master()
            return

        logger.debug("condor_off succeeded for {}: {}".format(self, off.stdout))

    def _terminate_condor_master(self):
        if not self.master_is_alive:
            return

        self.condor_master.terminate()
        logger.debug(
            "Sent terminate signal to condor_master (pid {}) for {}".format(
                self.condor_master.pid, self
            )
        )

    def _kill_condor_master(self):
        self.condor_master.kill()
        logger.debug(
            "Sent kill signal to condor_master (pid {}) for {}".format(
                self.condor_master.pid, self
            )
        )

    def _wait_for_master_to_terminate(self, kill_after=60, timeout=120):
        logger.debug(
            "Waiting for condor_master (pid {}) for {} to terminate".format(
                self.condor_master.pid, self
            )
        )

        start = time.time()
        killed = False
        while True:
            try:
                self.condor_master.communicate(timeout=5)
                break
            except TimeoutError:
                pass

            elapsed = time.time() - start

            if not killed:
                logger.debug(
                    "condor_master for {} has not terminated yet, will kill in {} seconds".format(
                        self, int(kill_after - elapsed)
                    )
                )

            if elapsed > kill_after and not killed:
                # TODO: in this path, we should also kill the other daemons
                # TODO: we can find their pids by reading the master log
                self._kill_condor_master()
                killed = True

            if elapsed > timeout:
                raise TimeoutError(
                    "Timed out while waiting for condor_master to terminate"
                )

        logger.debug(
            "condor_master (pid {}) for {} has terminated with exit code {}".format(
                self.condor_master.pid, self, self.condor_master.returncode
            )
        )

    def run_command(self, *args, **kwargs):
        """
        Execute a command with ``CONDOR_CONFIG`` set to point to this HTCondor pool.
        Arguments and keyword arguments are passed through to :func:`~run_command`.
        """
        with self.use_config():
            return run_command(*args, **kwargs)

    @property
    def master_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's master."""
        return self._get_log_path("MASTER")

    @property
    def collector_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's collector."""
        return self._get_log_path("COLLECTOR")

    @property
    def negotiator_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's negotiator."""
        return self._get_log_path("NEGOTIATOR")

    @property
    def schedd_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's schedd."""
        return self._get_log_path("SCHEDD")

    @property
    def startd_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's startd."""
        return self._get_log_path("STARTD")

    @property
    def shadow_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's shadows."""
        return self._get_log_path("SHADOW")

    @property
    def job_queue_log(self) -> Path:
        """The path to the pool's job queue log."""
        return self._get_log_path("JOB_QUEUE")

    @property
    def startd_address(self):
        """The address of the pool's startd."""
        return self._get_address_file("STARTD").read_text().splitlines()[0]

    def _get_log_path(self, subsystem: str) -> Path:
        return Path(self.get_config_val("{}_LOG".format(subsystem)))

    def _get_address_file(self, subsystem: str) -> Path:
        return Path(self.get_config_val("{}_ADDRESS_FILE".format(subsystem)))

    def get_config_val(self, variable: str) -> str:
        return self.run_command(["condor_config_val", str(variable)]).stdout


def set_env_var(key: str, value: str):
    os.environ[key] = value


def unset_env_var(key: str):
    value = os.environ.get(key, None)

    if value is not None:
        del os.environ[key]


class SetEnv:
    """
    A context manager for setting environment variables.
    Inside the context manager's block, the environment is updated according
    to the mapping. When the block ends, the environment is reset to whatever
    it was before entering the block.
    If you need to change the ``CONDOR_CONFIG``, use the specialized
    :func:`SetCondorConfig`.
    """

    UNSET = object()

    def __init__(self, mapping: Mapping[str, str]):
        self.mapping = mapping
        self.previous_values = None

    def __enter__(self):
        self.previous_values = {
            key: os.environ.get(key, self.UNSET) for key in self.mapping.keys()
        }
        os.environ.update(self.mapping)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for key, prev_val in self.previous_values.items():
            if prev_val is not self.UNSET:
                set_env_var(key, prev_val)
            else:
                unset_env_var(key)


class SetCondorConfig:
    """
    A context manager. Inside the block, the Condor config file is the one given
    to the constructor. After the block, it is reset to whatever it was before
    the block was entered.
    """

    def __init__(self, config_file: Path):
        self.config_file = Path(config_file)
        self.previous_value = None

    def __enter__(self):
        self.previous_value = os.environ.get("CONDOR_CONFIG", None)
        set_env_var("CONDOR_CONFIG", str(self.config_file))

        htcondor.reload_config()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.previous_value is not None:
            set_env_var("CONDOR_CONFIG", self.previous_value)
        else:
            unset_env_var("CONDOR_CONFIG")

        htcondor.reload_config()


def run_command(
    args: List[str], stdin=None, timeout: int = 60, log: bool = True,
):
    """
    Execute a command.

    Parameters
    ----------
    args
    stdin
    timeout
    log

    Returns
    -------

    """
    if timeout is None:
        raise TypeError("run_command timeout cannot be None")

    args = list(map(str, args))
    p = subprocess.run(
        args,
        timeout=timeout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=stdin,
        universal_newlines=True,
    )
    p.stdout = p.stdout.rstrip()
    p.stderr = p.stderr.rstrip()

    msg_lines = [
        "Ran command: {}".format(" ".join(p.args)),
        "CONDOR_CONFIG = {}".format(os.environ.get("CONDOR_CONFIG", "<not set>")),
        "exit code: {}".format(p.returncode),
        "stdout:{}{}".format("\n" if "\n" in p.stdout else " ", p.stdout),
        "stderr:{}{}".format("\n" if "\n" in p.stderr else " ", p.stderr),
    ]
    msg = "\n".join(msg_lines)

    if log and p.returncode != 0:
        logger.debug(msg)

    return p


def try_relative_to(path: Path, to: Path) -> Path:
    try:
        return path.relative_to(to)
    except ValueError:
        return path
