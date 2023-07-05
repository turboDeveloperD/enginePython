from unittest.mock import MagicMock

import pytest
from dataproc_sdk import DatioSchema

from exampleenginepythonqiyhbwvw.io.init_values import InitValues
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime, get_params_from_job_env
from pyspark.sql import *
from unittest import TestCase

class TestApp(TestCase):

    @pytest.fixture(autouse=True)
    def spark_session(self, spark_test):
        self.spark = spark_test

    def test_initialize_inputs(self):

        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")

        runtimeContext = MagicMock()
        runtimeContext.getConfig.return_value = config
        root_key = "EnvironmentVarsPM"
        parameters = get_params_from_runtime(runtimeContext, root_key)

        init_Values = InitValues()

        #customers_df, phones_df, output_path, output_schema = init_values.initialize_inputs(parameters)
        clients_df, contracts_df, products_df, out_path, out_schema = init_Values.initialize_inputs(parameters)

        self.assertEqual(type(clients_df), DataFrame)
        self.assertEqual(type(contracts_df), DataFrame)
        self.assertEqual(type(products_df), DataFrame)
        self.assertEqual(type(out_path), str)
        self.assertEqual(type(out_schema), DatioSchema)





