from unittest import TestCase
from unittest.mock import patch, mock_open, MagicMock

from exampleenginepythonqiyhbwvw.config import (
    get_params_from_job_env,
    get_params_from_config,
    get_params_from_runtime,
)


APPLICATION_CONF_MOCK: str = """
    {
        config {
            B_SIZE = ${?B_SIZE_OVERWRITE}
            EPOCHS = ${?EPOCHS_OVERWRITE}
        }
    }
"""

EMPTY_APPLICATION_CONF_MOCK: str = """
    {
        application="empty"
    }
"""


class ConfigTestCase(TestCase):

    """
    Test case for a Python entrypoint execution.
    """

    def test__get_params_from_config__empty_conf(self) -> None:
        run_params = '[{"B_SIZE_OVERWRITE": "40", "EPOCHS_OVERWRITE": "5"}]'

        with patch(
            "os.getenv",
            return_value=run_params,
        ), patch(
            "builtins.open",
            mock_open(read_data=EMPTY_APPLICATION_CONF_MOCK),
        ):
            parameters = get_params_from_config("foo.conf")

        self.assertEqual(parameters, {})

    def test__get_params_from_config__empty_env(self) -> None:
        run_params = '[{"B_SIZE_OVERWRITE": "40", "EPOCHS_OVERWRITE": "5"}]'

        with patch(
            "os.getenv",
            return_value=run_params,
        ), patch(
            "builtins.open",
            mock_open(read_data=APPLICATION_CONF_MOCK),
        ):
            parameters = get_params_from_config("foo.conf")

        self.assertEqual(parameters, {
            "B_SIZE": "40",
            "EPOCHS": "5",
        })

    def test__get_params_from_config__overrides(self) -> None:
        run_params = '[{"B_SIZE_OVERWRITE": "50"}]'

        with patch(
            "os.getenv",
            return_value=run_params,
        ), patch(
            "builtins.open",
            mock_open(read_data=APPLICATION_CONF_MOCK),
        ):
            parameters = get_params_from_config("foo.conf")

        self.assertEqual(parameters, {
            "B_SIZE": "50",
        })

    def test__get_params_from_runtime__overrides(self) -> None:
        value = "foo"
        params = {
            "foo": value,
            "bar": value,
        }
        runtime = MagicMock()
        runtime.getConfig.return_value.getObject.return_value = params
        runtime.getConfig.return_value.getString.return_value = value
        runtime.getConfig.return_value.isEmpty.return_value = False

        parameters = get_params_from_runtime(runtime, "foo")

        self.assertEqual(parameters, params)

