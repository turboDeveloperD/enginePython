import sys
from behave import Given, Then, When
from behave.runner import Context
from worker import main


@Given(u'a config file {config_file}')
def set_config(context: Context, config_file):
    conf_path = "%s/%s" % (context.resources_dir, config_file)
    context.config_file = conf_path


@When(u'execute example app file in PySpark')
def execute_app(context: Context):
    sys.argv = ["local", context.config_file]
    context.return_code = main()


@Then(u'result should be {exit_code}')
def check_exit(context: Context, exit_code):
    assert context.return_code == int(exit_code)
