import logging
import os
import sys
from tempfile import NamedTemporaryFile
from unittest import TestCase
from unittest.mock import MagicMock, patch

from worker import main, InitTask

from _pytest.monkeypatch import MonkeyPatch


class TestWorker(TestCase):
    """
    Test class for Dataproc job entrypoint script
    """

    @patch(
        "worker.get_user_logger",
		return_value=logging.getLogger(),
    )
    def setUp(self, logger):
        self.monkeypatch = MonkeyPatch()

    def test_worker_no_args(self):
        """
        Test worker entrypoint execution with no arguments
        """

        sys.argv = [__file__]

        ret_code = main()

        self.assertEqual(ret_code, -1)

    def test_main(self):
        """
        Test worker entrypoint execution with config.
        """

        config = MagicMock()
        cfg_file = NamedTemporaryFile(prefix="application", suffix=".conf")
        sys.argv = [__file__, cfg_file.name]

        config.getString = MagicMock(return_value=os.path.realpath(__file__))

        with patch(
			"exampleenginepythonqiyhbwvw.app.Main.main", return_value=0
		), patch(
            "dataproc_sdk.dataproc_sdk_launcher.launcher.SparkLauncher.execute",
            return_value=0,
        ):
            ret_code = main()

        self.assertEqual(ret_code, 0)

    def test_getProcessId(self):
        """
        Test getProcessId is returning the correct method name.
        """

        init_task = InitTask()

        self.assertEqual(init_task.getProcessId(), "InitTask")
