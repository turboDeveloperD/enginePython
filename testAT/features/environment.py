import os
import sys
from os.path import abspath, dirname, realpath
from behave.model import Scenario
from behave.runner import Context
import glob
from pathlib import Path
from pyspark.sql import SparkSession


def before_all(context: Context):
    context.main_dir = abspath("%s/../.." % dirname(__file__))
    context.resources_dir = realpath("%s/resources" % context.main_dir)
    os.environ['RESOURCES_DIR'] = context.resources_dir

    jars_path = str(Path(sys.prefix) / 'share' / 'sdk' / 'jars' / 'dataproc-sdk-all-*.jar')
    jars_list = glob.glob(jars_path)
    if jars_list:
        sdk_path = jars_list[0]
    else:
        raise FileNotFoundError(f"Dataproc SDK jar not found in {jars_path}, have you installed the requirements?")

    # Initialize Spark session
    SparkSession.builder \
        .appName("testAT") \
        .config("spark.jars", sdk_path) \
        .master("local[*]") \
        .getOrCreate()


def before_scenario(context: Context, scenario: Scenario):
    pass


def after_step(context, step):
    # Required to avoid some issue in behave no printing the last line
    print()
