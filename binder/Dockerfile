# Copyright 2019 HTCondor Team, Computer Sciences Department,
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

FROM htcondor/htc-scipy-notebook:latest

USER root

RUN : \
 && apt-get update \
 && apt-get install -y psmisc \
 && apt-get clean -y \
 && rm -rf /var/lib/apt/lists/* \
 && echo 'exec "$@"' > /.entrypoint \
 && rm /home/jovyan/.condor/user_config \
 && :

USER ${NB_UID}:${NB_GID}

WORKDIR ${HOME}/demo
COPY . ${HOME}/demo
