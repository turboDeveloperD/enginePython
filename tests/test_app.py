import logging
from unittest import TestCase
from unittest.mock import PropertyMock, MagicMock, patch
import os


from exampleenginepythonqiyhbwvw.app import Main

from py4j.java_gateway import GatewayProperty, GatewayClient, JavaObject
from py4j.protocol import Py4JJavaError


class TestApp(TestCase):
    """
    Test class for Dataproc Pyspark job entrypoint execution
    """



    def test_app_empty_config(self):
        """
        Test app entrypoint execution with empty config file
        """

        runtimeContext = MagicMock()
        runtimeContext.getConfig.return_value.getObject.side_effect = Exception
        runtimeContext.getConfig.return_value.getString.return_value = "empty"
        #config = MagicMock()
        app_main = Main()

        """
        gateway_property = GatewayProperty(auto_field="Mock", pool="Mock")
        client = GatewayClient(gateway_property=gateway_property)
        java_object = JavaObject("RunTimeError", client)
        config.getObject = MagicMock(
            side_effect=Py4JJavaError("no params", java_object)
        )
        config.getString = MagicMock(return_value="empty")
        runtimeContext.getConfig = MagicMock(return_value=config)"""

        if os.path.exists("./exampleenginepythonqiyhbwvw/dataflow.py"):
            with patch("exampleenginepythonqiyhbwvw.app.dataproc_dataflow"):
                ret_code = app_main.main(runtimeContext)
        else:
            with patch("exampleenginepythonqiyhbwvw.app.DataprocExperiment"):
                ret_code = app_main.main(runtimeContext)

        self.assertEqual(ret_code, 0)

    def test_app_unknown_error(self):
        """
        Test app entrypoint execution with Exception
        """

        runtimeContext = MagicMock()
        app_main = Main()

        runtimeContext.getConfig = MagicMock(side_effect=Exception())

        ret_code = app_main.main(runtimeContext)

        self.assertEqual(ret_code, -1)

    def test_app_config(self):
        """
        Test app entrypoint execution with config
        """

        runtimeContext = MagicMock()
        app_main = Main()

        if os.path.exists("./exampleenginepythonqiyhbwvw/dataflow.py"):
            with patch("exampleenginepythonqiyhbwvw.app.dataproc_dataflow"):
                ret_code = app_main.main(runtimeContext)
        else:
            with patch("exampleenginepythonqiyhbwvw.app.DataprocExperiment"):
                ret_code = app_main.main(runtimeContext)

        self.assertEqual(ret_code, 0)
