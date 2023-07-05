from genericpath import exists
import os
from unittest import TestCase
from unittest.mock import patch, mock_open, Mock


from exampleenginepythonqiyhbwvw.main import main


MOCK_EMPTY_CONFIG = """
{
    application="empty"
}
"""

MOCK_CONFIG = """
{
    config {
        batch_size = ${?batch_size_OVERWRITE}
        epochs = ${?epochs_OVERWRITE}
    }
}
"""

APPLICATION_CONF_MOCK: str = """
{
    config {
        n_elem = ${?n_elem_OVERWRITE}
        choice = ${?choice_OVERWRITE}
    }
}
"""
dataflow_path = os.path.join(os.path.dirname(__file__), "dataflow.py")

class TestMain(TestCase):

    """
    Test class for python entrypoint execution test
    """

    
    
    def test_main_config(self):

        """
            Test main entrypoint execution with parameters
            """

        if os.path.exists("./exampleenginepythonqiyhbwvw/dataflow.py"):
            with patch(
                "os.getenv",
                return_value='[{"n_elem_OVERWRITE":"40", "choice_OVERWRITE":"5"}]',
            ), patch("os.path.isfile", side_effect=[True, True]
            ), patch("os.path.join", return_value=os.getcwd()
            ), patch(
                "builtins.open", mock_open(read_data=APPLICATION_CONF_MOCK)
            ), patch(
                "exampleenginepythonqiyhbwvw.dataflow.dataproc_dataflow", autospec=True,
            ) as mock:
                mock.run_dataproc = Mock()
                return_code = main()

            mock.run_dataproc.assert_called_once_with(n_elem="40", choice="5")
            self.assertEqual(return_code, 0)
        else:
            pass
    
    def test_main_config_experiment(self):
        if os.path.exists("./exampleenginepythonqiyhbwvw/dataflow.py"):
            pass
        else:
            with patch(
                "os.getenv",
                side_effect=[
                    os.getcwd(),
                    '[{"batch_size_OVERWRITE":"40", "epochs_OVERWRITE":"5"}]',
                    '[{"batch_size_OVERWRITE":"40", "epochs_OVERWRITE":"5"}]',
                ],
            ), patch("os.path.isfile", side_effect = [ True, False]
            ), patch(
                "builtins.open", mock_open(read_data=MOCK_CONFIG)
            ), patch(
                "exampleenginepythonqiyhbwvw.experiment.DataprocExperiment", autospec=True,
            ) as mock_experiment:
                mock_experiment_instance = mock_experiment.return_value
                return_code = main()

            mock_experiment_instance.run.assert_called_once_with(batch_size="40", epochs="5")
            self.assertEqual(return_code, 0)

    def test_main_config_empty_param(self):

        """
        Test main entrypoint execution with parameters
        """

        if os.path.exists("./exampleenginepythonqiyhbwvw/dataflow.py"):
            with patch(
                "os.getenv",
                return_value='[{"choice_OVERWRITE":"5"}]',
            ), patch("os.path.isfile", return_value=True
            ), patch("os.path.join", return_value=os.getcwd()), patch(
                "builtins.open", mock_open(read_data=APPLICATION_CONF_MOCK)
            ), patch(
                "exampleenginepythonqiyhbwvw.dataflow.dataproc_dataflow", autospec=True,
            ) as mock:
                mock.run_dataproc = Mock()
                return_code = main()
            mock.run_dataproc.assert_called_once_with(choice="5")
            self.assertEqual(return_code, 0)
        else:
            pass
 
    def test_main_config_empty_param_experiment(self):

        """
        Test main entrypoint execution with parameters
        """

        if os.path.exists("./exampleenginepythonqiyhbwvw/dataflow.py"):
            pass
        else:
            with patch(
                "os.getenv",
                side_effect=[
                    os.getcwd(),
                    '[{"batch_size_OVERWRITE":"", "epochs_OVERWRITE":"5"}]',
                    '[{"batch_size_OVERWRITE":"", "epochs_OVERWRITE":"5"}]',
                ],
            ), patch("os.path.isfile", side_effect = [ True, False ]
            ), patch("builtins.open", mock_open(read_data=MOCK_CONFIG)
            ), patch(
                "exampleenginepythonqiyhbwvw.experiment.DataprocExperiment", autospec=True,
            ) as mock_experiment:
                mock_experiment_instance = mock_experiment.return_value
                return_code = main()

            mock_experiment_instance.run.assert_called_once_with(epochs="5")
            self.assertEqual(return_code, 0)

    def test_main_no_config(self):

        """
        Test main entrypoint failure when config no exist
        """

        with patch("os.getenv", return_value=os.getcwd()), patch(
            "os.path.isfile", return_value=False
        ):
            return_code = main()

        self.assertEqual(return_code, -1)
