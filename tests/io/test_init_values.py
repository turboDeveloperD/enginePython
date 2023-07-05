from unittest.mock import MagicMock

from dataproc_sdk import DatioSchema

from exampleenginepythonqiyhbwvw.io.init_values import InitValues
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime, get_params_from_job_env
from pyspark.sql import DataFrame
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o
import exampleenginepythonqiyhbwvw.common.constants as c

def test_initialize_inputs(spark_test):

    config_loader = spark_test._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
    config  = config_loader.fromPath("resources/application.conf")
    runtimeContext = MagicMock()
    runtimeContext.getConfig.return_value = config
    root_key = "EnvironmentVarsPM"
    parameters = get_params_from_runtime(runtimeContext, root_key)

    init_Values = InitValues()

    clients_df, contracts_df, products_df, out_path, out_schema = init_Values.initialize_inputs(parameters)

    assert type(clients_df) == DataFrame
    assert type(contracts_df) == DataFrame
    assert type(products_df) == DataFrame
    assert type(out_path) == str
    assert type(out_schema) == DatioSchema

    assert products_df.columns == [i.cod_producto.name, i.desc_producto.name]

def test_get_input_df(spark_test):

    config_loader = spark_test._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
    config  = config_loader.fromPath("resources/application.conf")
    runtimeContext = MagicMock()
    runtimeContext.getConfig.return_value = config
    root_key = "EnvironmentVarsPM"
    parameters = get_params_from_runtime(runtimeContext, root_key)

    init_Values = InitValues()
    clients_df = init_Values.get_input_df(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)
    contracts_df = init_Values.get_input_df(parameters, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)
    products_df = init_Values.get_input_df(parameters, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)

    assert type(clients_df) == DataFrame
    assert type(contracts_df) == DataFrame
    assert type(products_df) == DataFrame
    assert clients_df.columns == [i.cod_client.name, i.nombre.name, i.edad.name, i.provincia.name, i.cod_postal.name,
                                  i.vip.name]
    assert contracts_df.columns == [i.cod_iuc.name, i.cod_titular.name, i.cod_producto.name, i.fec_alta.name,
                                  i.activo.name]


def test_get_config_by_name(spark_test):

    config_loader = spark_test._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
    config  = config_loader.fromPath("resources/application.conf")
    runtimeContext = MagicMock()
    runtimeContext.getConfig.return_value = config
    root_key = "EnvironmentVarsPM"
    parameters = get_params_from_runtime(runtimeContext, root_key)

    init_Values = InitValues()
    out_path, out_schema = init_Values.get_config_by_name(parameters, c.OUTPUT_PATH, c.OUTPUT_SCHEMA)

    assert type(out_path) == str
    assert type(out_schema) == DatioSchema

