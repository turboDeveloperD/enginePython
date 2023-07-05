from unittest import TestCase
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import  pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import *
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o

import pytest


class Test_business_logic(TestCase):
    """def spark_session(self, clients_df, clients_dummy_df, products_df, contracts_dummy_df, products_dummy_df,
                              business_logic):"""
    @pytest.fixture(autouse=True)
    def spark_session(self, clients_df, clients_dummy_df, products_df, contracts_dummy_df, products_dummy_df,
                      clients_dummy_df_2, business_logic):

        self.clients_df = clients_df
        self.clients_dummy_df = clients_dummy_df
        self.contracts_dummy_df = contracts_dummy_df
        self.products_dummy_df = products_dummy_df
        self.clients_dummy_df_2 = clients_dummy_df_2
        self.business_logic = business_logic


    """def test_filter_by_age_and_vip(self):
        self.clients_filtered_df = self.business_logic.filter_by_age_and_vip(self.clients_df)
        self.assertEqual(self.clients_filtered_df.filter(i.edad() < c.THIRTY_FIVE_NUMBER).count(), 0 )
        self.assertEqual(self.clients_filtered_df.filter(i.edad() > c.FIFTY_NUMBER).count(), 0)
        #self.assertEqual(self.clients_filtered_df.filter(i.vip() != c.TRUE_VALUE).count(), 0)


    def test_join_tables(self):
        join_df = self.business_logic.\
            join_tables(self.clients_dummy_df, self.contracts_dummy_df, self.products_dummy_df)
        total_expected_columns = len(self.clients_dummy_df.columns) \
                                 + len(self.contracts_dummy_df.columns) \
                                 + len(self.products_dummy_df.columns) - 1

        self.assertEqual(len(join_df.columns), total_expected_columns)


    def test_hash_columns(self):
        output_df = self.business_logic.hash_columns(self.contracts_dummy_df)
        self.assertEqual(len(output_df.columns), len(self.contracts_dummy_df.columns) + 1)"""


    def test_filter_by_number_of_contracts(self):
        output_df = self.business_logic.filter_by_number_of_contracts(self.clients_dummy_df_2)

        validation_df = output_df.select(*output_df.columns,
                                         f.count(i.cod_client())
                                         .over(Window.partitionBy( i.cod_client() ) ).alias(c.COUNT_COLUMN) )\
            .filter( f.col(c.COUNT_COLUMN ) <= c.THREE_NUMBER)

        #self.assertEqual(0, 0)
        self.assertEqual(validation_df.count(), 0)

        #self.assertEqual(output_df.count(), 4)
