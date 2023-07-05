import sys

from dataproc_sdk.dataproc_sdk_api.pyspark_process import PySparkProcess
from dataproc_sdk.dataproc_sdk_launcher.launcher import SparkLauncher
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from py4j.java_gateway import JavaObject
from exampleenginepythonqiyhbwvw import app


# THIS FILE CANNOT BE EDIT OR DELETED

class InitTask(PySparkProcess):
    """
    InitTask initializes pysparkutils methods
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(InitTask.__qualname__)

    def getProcessId(self) -> str:
        return "InitTask"

    def runProcess(self, runtimeContext: JavaObject) -> int:
        """
        Execute the task
        :param runtimeContext: the applicable runtime context
        :return: an exit code that describes the process results
        """

        self.__logger.info("Starting runProcess method:")
        process_main = app.Main()
        return process_main.main(runtimeContext)


def main() -> int:
    """
    Application entry point.

    Returns:
        int: exit code.
    """
    if len(sys.argv) != 2:
        print("Usage: spark-submit <MASTER_OPTIONS> worker.py <CONFIG_FILE>",
              file=sys.stderr)
        return -1

    ret_code = SparkLauncher().execute(InitTask, [sys.argv[1], 'InitTask'])
    print("#########################")
    print("Exit code: %s" % ret_code)
    print("#########################")
    return ret_code


if __name__ == "__main__":
    sys.exit(main())
