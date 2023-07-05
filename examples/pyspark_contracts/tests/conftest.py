# Remove it under your own responsibility.
# It is used in order to pytest find the correct python test files.
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_test():
    """
    Executes the module setup and initializes the SparkSession
    """
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()