from exampleenginepythonqiyhbwvw.business.business_logic import BusinessLogic
import pytest


@pytest.fixture(scope="module")
def bl():
    bl = BusinessLogic()
    yield bl

@pytest.fixture(scope="module")
def clients_df(spark_test):
    input_clients_values = [('00001', 'Julen', 31, 'Valencia', '23700', 'true'),
                        ('00002', 'Javier', 58, 'Jaén', '08728', 'true'),
                        ('00003', 'Maria', 38, 'Gerona', '18393', 'true'),
                        ('00004', 'Jordi', 38, 'Barcelona', '28473', 'false')]
    clients_df = spark_test.createDataFrame(input_clients_values, ['cod_client',
                                                          'nombre',
                                                          'edad',
                                                          'provincia',
                                                          'cod_postal',
                                                          'vip'])
    yield clients_df


@pytest.fixture(scope="module")
def contracts_df(spark_test):
    input_contracts_values = [('30000', '00001', '100', '2012-05-01', 'true'),
                          ('30001', '00001', '200', '2014-05-01', 'true'),
                          ('30002', '00001', '300', '2006-02-01', 'false'),
                          ('30003', '00001', '150', '2012-05-01', 'true'),
                          ('30004', '00003', '300', '2012-05-01', 'true'),
                          ('30005', '00003', '400', '2012-05-01', 'true'),
                          ('30006', '00003', '150', '2019-10-14', 'false')]
    contracts_df = spark_test.createDataFrame(input_contracts_values, ['cod_iuc',
                                                              'cod_titular',
                                                              'cod_producto',
                                                              'fec_alta',
                                                              'activo'])
    yield contracts_df


@pytest.fixture(scope="module")
def products_df(spark_test):
    input_products_values = [(100, 'tarjeta crédito'),
                         (150, 'tarjeta débito'),
                         (200, 'préstamo personal'),
                         (300, 'hipoteca'),
                         (400, 'plan de pensiones')]
    products_df = spark_test.createDataFrame(input_products_values, ['cod_producto',
                                                            'desc_producto'])
    yield products_df


def test_filter(bl, clients_df):
    # GIVEN
    # WHEN
    output_df = bl.filter_example(clients_df)
    # THEN
    assert output_df.count() == 2
    assert output_df.columns == ['cod_client',
                                 'nombre',
                                 'edad',
                                 'provincia',
                                 'cod_postal',
                                 'vip']


def test_join(bl, clients_df, contracts_df, products_df):
    # GIVEN
    # WHEN
    output_df = bl.join_example(clients_df, contracts_df, products_df)
    # THEN
    assert output_df.count() == 7
    assert output_df.columns == ['cod_producto',
                                 'cod_iuc',
                                 'cod_titular',
                                 'fec_alta',
                                 'activo',
                                 'cod_client',
                                 'nombre',
                                 'edad',
                                 'provincia',
                                 'cod_postal',
                                 'vip',
                                 'desc_producto']


def test_filter_sql(spark_test, bl, clients_df, contracts_df, products_df):
    # GIVEN
    clients_contracts_df = bl.join_example(clients_df, contracts_df, products_df)
    clients_contracts_df.createOrReplaceTempView("clients_contracts_df")
    # WHEN
    output_df = bl.filter_sql_example(spark_test)
    # THEN
    assert output_df.count() == 4


def test_add_hash(bl, clients_df):
    # GIVEN
    # WHEN
    output_df = bl.add_hash(clients_df)
    # THEN
    assert len(output_df.columns) == len(clients_df.columns) + 1


def test_send_notification(bl, clients_df, contracts_df, products_df):
    # GIVEN
    clients_contracts_df = bl.join_example(clients_df, contracts_df, products_df)
    # WHEN
    output_list = bl.send_notification(clients_contracts_df)
    # THEN
    assert len(output_list) == 2


@pytest.mark.parametrize("value,binary_column, column_bigger", [(250, "cod_producto_B", 3), (550, "cod_producto_B", 0)])
def test_add_binary_column(bl, contracts_df, value, binary_column, column_bigger):
    """
    @pytest.mark.parametrize: Usage example.
    """
    # GIVEN
    # WHEN
    df = bl.add_binary_column(contracts_df, binary_column, "cod_producto", value)
    # THEN
    assert df.filter("cod_producto_B = 1").count() == column_bigger