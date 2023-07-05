#!/usr/bin/env python3

import os
import shlex
import subprocess
import sys

"""
PyKaa installation script
=========================

This script install PyKaa workflow tool from official repositories

Requirements
------------

- Python 3
- Artifactory credentials as environment variables:
   - ARTIFACTORY_USER_PROFILE
   - ARTIFACTORY_API_KEY

In case the environment variables are not provided, this script will ask you to enter them
"""


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def _check_env():
    vars = ("ARTIFACTORY_USER_PROFILE", "ARTIFACTORY_API_KEY")
    for v in vars:
        if v not in os.environ:
            msg = "%s:" % v
            os.environ[v] = input(bcolors.BOLD + msg + bcolors.ENDC)
    return


def _install_pykaa() -> None:
    """
    Download pykaa library
    return: a Path object pointing to PyKaa library
    """
    user_flag = "" if "VIRTUAL_ENV" in os.environ else "--user"
    credentials = "%s:%s" % (os.environ["ARTIFACTORY_USER_PROFILE"], os.environ["ARTIFACTORY_API_KEY"])
    uri = "https://%s@artifactory.globaldevtools.bbva.com/artifactory/api/pypi/gl-datio-runtime-pypi-local/simple/" % (credentials)
    command = "pip install %s pykaa==0.5.2 pykaa-dataproc -U --extra-index-url %s" % (user_flag, uri)
    subprocess.run(shlex.split(command), check=True)


if __name__ == "__main__":
    _check_env()
    try:
        pykaa_file = _install_pykaa()
    except subprocess.CalledProcessError as cpe:
        print(cpe.output)
        print(bcolors.FAIL + "PyKaa could not be installed" + bcolors.ENDC)
        sys.exit(cpe.returncode)
    else:
        OK_SUCCESS = bcolors.BOLD + bcolors.OKGREEN
        print(OK_SUCCESS + "PyKaa installed correctly!" + bcolors.ENDC)
        print("Type kaa -h to start")
        sys.exit(0)
