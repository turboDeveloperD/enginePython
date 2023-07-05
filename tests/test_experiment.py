from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment


class TestApp(TestCase):

    def test_run_experiment(self):
        parameters = {
            "phone_path" : "resources/data/input/parquet/t_fdev_phones",
            "phone_schema" : "resources/schemas/t_fdev_phones.output.schema",
            "customers_path" : "resources/data/input/parquet/t_fdev_customers",
            "customers_schema" : "resources/schemas/t_fdev_customers.output.schema",
            "output_customersphones" : "resources/data/output",
            "output_customersphones_schema" : "resources/schemas/t_fdev_customersphones.output.schema",
            "date" : "2023-01-01",
            "output" : "resources/data/output/jwk_date=2023-01-01"
        }

        experiment = DataprocExperiment()
        experiment.run(**parameters)

        spark = SparkSession.builder.appName("unittest_job").master("local[*]").getOrCreate()

        out_df = spark.read.parquet(parameters["output"])

        self.assertIsInstance(out_df, DataFrame)
