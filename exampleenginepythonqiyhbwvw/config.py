import json
import os
import re
from typing import Dict
from py4j.java_gateway import JavaObject
from py4j.protocol import Py4JJavaError


def get_params_from_runtime(runtimeContext: JavaObject, root_key: str) -> Dict:
    """
    Returns parameters from pyspark context

    Args:
        runtimeContext: The pyspark context parameters
        root_key: The key where params are setted

    Returns:
        The pyspark context parameters
    """
    config = runtimeContext.getConfig()

    if config.isEmpty():
        return {}

    try:
        return {
            param: config.getString(f"{root_key}.{param}")
            for param in config.getObject(f"{root_key}")
            if config.getString(f"{root_key}.{param}") != ""
        }
    except Py4JJavaError:
        return {}


def get_params_from_job_env(config_path: str) -> Dict:
    """
    Returns environment variables from os environment

    Args:
        config_path: The config file parameters

    Returns:
        The environment variables found, empty if the configuration is empty
    """
    env_params = {}  # Empty dictionary to add values into

    OVERWRITE_SUFFIX = "_OVERWRITE"

    for item, value in os.environ.items():
        env_var = item.endswith(OVERWRITE_SUFFIX)
        if env_var:
            env_var_overwrite = "${?" + item + "}"
            env_var_original = item.replace(OVERWRITE_SUFFIX, "")
            with open(config_path, "r") as f:
                content = f.read()
                if env_var_overwrite in content:
                    env_params[env_var_original] = value

    return env_params


def get_params_from_config(config_path: str) -> Dict:
    """
    Returns parameters from file

    Args:
        config_path: The config file parameters

    Returns:
        The config file parameters
    """
    pattern = re.compile("(\S*)\s*[:=]\s*\$\{\??(\S*)}")  # noqa: W605
    parameters = {}
    run_params = json.loads(os.getenv("RUN_PARAMS", "[{}]"))

    for line in open(config_path).readlines():
        match = re.search(pattern, line)
        if match is not None:
            env_param = str(run_params[0].get(match.group(2), ""))
            if env_param is not None and env_param != "":
                parameters[str(match.group(1)).replace("\"", "")] = env_param

    if not parameters:
        parameters = get_params_from_job_env(config_path)

    return parameters
