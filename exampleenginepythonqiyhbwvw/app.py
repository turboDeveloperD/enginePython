import os
import json

os.environ["ENMA_CONFIG_PATH"] = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), ".sandbox"
)
from py4j.java_gateway import JavaObject
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime, get_params_from_job_env


if os.path.isfile(os.path.join(os.path.dirname(__file__), "dataflow.py")):
    from exampleenginepythonqiyhbwvw.dataflow import dataproc_dataflow  # noqa: E402
else:
    from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment  # noqa: E402


class Main:
    """
    Main method for executing PySpark example process
    """

    @staticmethod
    def methodMe(self, dir):
        with open(dir) as the_file:
            jsonObject = json.load(the_file)
            the_file.close()

        print(jsonObject['inputs'])


    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(Main.__qualname__)

    def main(self, runtimeContext: JavaObject) -> int:
        """
        THIS METHOD CANNOT BE REMOVED
        Application entry point
        """
        ret_code = 0
        parameters = {}

        ## Fin Cambio ejercicio 3
        """val1 = runtimeContext.getConfig()
        print(val1)
        keyroot = "inputs"
        k2 = ""
        dict1 = { }
        #params = val1.config.getString(f"{keyroot}")
        for param in val1.getObject(f"{keyroot}"):
            #print(param)
            value = val1.getObject(f"{keyroot}.{param}")
            #print(value)
            #print(type(value))
            dict1[param] = value

        print(type(dict1))
        print(dict1)

        for item in dict1:
            print(dict1[item])
        """
        ## Fin Cambio ejercicio 3
        # PART 1 - READ FROM CONFIGURATION
        # Reading config file for input and output paths
        try:
            config = runtimeContext.getConfig()
            print(config.getString("params.date"))
            if not config.isEmpty():
                root_key = "EnvironmentVarsPM" # original
                #root_key = "inputs" # mio
                parameters = get_params_from_runtime(runtimeContext, root_key)
        except Exception as e:
            self.__logger.error(e)
            return -1

        self.__logger.info(f"parameters1: {parameters}")

        # PART 2 - BUSSINESS LOGIC
        try:
            self.__logger.info("Started 'run' method")
            dataflow_path = os.path.join(os.path.dirname(__file__), "dataflow.py")
            if os.path.isfile(dataflow_path):
                self.__logger.info("Executing dataflow code")
                dataproc_dataflow.run_dataproc(**parameters)
                self.__logger.info("Dataflow code executed")
            else:
                self.__logger.info("Executing experiment code")
                entrypoint = DataprocExperiment()
                entrypoint.run(**parameters)
                self.__logger.info("Experiment code executed")
            self.__logger.info("Ended 'run' method")
        except Exception as e:
            ret_code = -1
            self.__logger.error(e)

        return ret_code
