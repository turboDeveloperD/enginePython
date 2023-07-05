import os
import sys

os.environ["ENMA_CONFIG_PATH"] = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), ".sandbox"
)
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from exampleenginepythonqiyhbwvw.config import get_params_from_config


def main():
    ret_code = 0
    logger = get_user_logger(main.__qualname__)
    config_path = os.path.join(os.getenv("PYTHON_HOME"), "application.conf")

    if not os.path.isfile(config_path):
        logger.error("Config file not found in specified path")
        return -1

    try:
        parameters = get_params_from_config(config_path)
        logger.info("Parameters: {}".format(parameters))
        logger.info("Started 'run' method")
        dataflow_path = os.path.join(os.path.dirname(__file__), "dataflow.py")
        if os.path.isfile(dataflow_path):
            logger.info("Executing dataflow code")
            from exampleenginepythonqiyhbwvw.dataflow import dataproc_dataflow
            dataproc_dataflow.run_dataproc(**parameters)
            logger.info("Dataflow code executed")
        else:
            logger.info("Executing experiment code")
            from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment
            entrypoint = DataprocExperiment()
            entrypoint.run(**parameters)
            logger.info("Experiment code executed")
        logger.info("Ended 'run' method")
    except Exception as e:
        logger.error(e)
        ret_code = -1

    return ret_code


if __name__ == "__main__":
    sys.exit(main())
