"""Unit tests for tap_gitlab.__init__ — do_discover() and main()."""
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch


def _make_parsed_args(config=None, state=None, discover=False, catalog=None):
    args = MagicMock()
    args.config = config or {"private_token": "tok", "start_date": "2023-01-01T00:00:00Z",
                              "groups": "", "projects": "123"}
    args.state = state
    args.discover = discover
    args.catalog = catalog
    return args


class TestDoDiscover(unittest.TestCase):

    @patch("tap_gitlab.discover")
    def test_do_discover_writes_catalog_to_stdout(self, mock_discover):
        from tap_gitlab import do_discover
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        captured = StringIO()
        with patch("sys.stdout", captured):
            do_discover()

        output = captured.getvalue()
        self.assertIn("streams", output)

    @patch("tap_gitlab.discover")
    def test_do_discover_calls_discover_once(self, mock_discover):
        from tap_gitlab import do_discover
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        with patch("sys.stdout", StringIO()):
            do_discover()

        mock_discover.assert_called_once()


class TestMain(unittest.TestCase):

    @patch("tap_gitlab.Client")
    @patch("singer.utils.parse_args")
    def test_main_calls_do_discover_when_discover_flag(self, mock_parse_args, mock_client_cls):
        from tap_gitlab import main, do_discover
        mock_parse_args.return_value = _make_parsed_args(discover=True)

        mock_client_instance = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        with patch("tap_gitlab.do_discover") as mock_dd, \
             patch("sys.stdout", StringIO()):
            main()
            mock_dd.assert_called_once()

    @patch("tap_gitlab.Client")
    @patch("singer.utils.parse_args")
    def test_main_calls_sync_when_catalog_provided(self, mock_parse_args, mock_client_cls):
        from tap_gitlab import main
        mock_catalog = MagicMock()
        mock_parse_args.return_value = _make_parsed_args(catalog=mock_catalog)

        mock_client_instance = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        with patch("tap_gitlab.sync") as mock_sync:
            main()
            mock_sync.assert_called_once()

    @patch("tap_gitlab.Client")
    @patch("singer.utils.parse_args")
    def test_main_uses_state_from_parsed_args(self, mock_parse_args, mock_client_cls):
        from tap_gitlab import main
        existing_state = {"bookmarks": {"projects": {"updated_at": "2023-06-01T00:00:00Z"}}}
        mock_catalog = MagicMock()
        mock_parse_args.return_value = _make_parsed_args(state=existing_state, catalog=mock_catalog)

        mock_client_instance = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        with patch("tap_gitlab.sync") as mock_sync:
            main()
            _, call_kwargs = mock_sync.call_args
            self.assertEqual(call_kwargs["state"], existing_state)

    @patch("tap_gitlab.Client")
    @patch("singer.utils.parse_args")
    def test_main_defaults_state_to_empty_dict(self, mock_parse_args, mock_client_cls):
        from tap_gitlab import main
        args = _make_parsed_args(catalog=MagicMock())
        args.state = None
        mock_parse_args.return_value = args

        mock_client_instance = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        with patch("tap_gitlab.sync") as mock_sync:
            main()
            _, call_kwargs = mock_sync.call_args
            self.assertEqual(call_kwargs["state"], {})
