"""Unit tests for tap_gitlab.sync — update_currently_syncing, write_schema, sync()."""
import unittest
from unittest.mock import MagicMock, patch

import singer
from singer.catalog import Catalog, CatalogEntry, Schema


def _make_stream_obj(tap_stream_id, parent="", children=None, is_selected=True):
    stream = MagicMock()
    stream.tap_stream_id = tap_stream_id
    stream.parent = parent
    stream.children = children or []
    stream.child_to_sync = []
    stream.is_selected.return_value = is_selected
    stream.sync.return_value = 5
    return stream


class TestUpdateCurrentlySyncing(unittest.TestCase):

    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    def test_sets_currently_syncing(self, mock_set, mock_write):
        from tap_gitlab.sync import update_currently_syncing
        state = {}
        update_currently_syncing(state, "projects")
        mock_set.assert_called_once_with(state, "projects")
        mock_write.assert_called_once_with(state)

    @patch("singer.write_state")
    @patch("singer.get_currently_syncing", return_value="projects")
    def test_clears_currently_syncing_when_stream_name_is_none(self, mock_get, mock_write):
        from tap_gitlab.sync import update_currently_syncing
        state = {"currently_syncing": "projects"}
        update_currently_syncing(state, None)
        self.assertNotIn("currently_syncing", state)
        mock_write.assert_called_once_with(state)

    @patch("singer.write_state")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_no_key_deleted_when_not_syncing(self, mock_get, mock_write):
        from tap_gitlab.sync import update_currently_syncing
        state = {}
        update_currently_syncing(state, None)
        mock_write.assert_called_once_with(state)


class TestWriteSchema(unittest.TestCase):

    def test_write_schema_calls_stream_write_schema_when_selected(self):
        from tap_gitlab.sync import write_schema
        stream = _make_stream_obj("projects", children=[])
        stream.is_selected.return_value = True

        catalog = MagicMock()
        write_schema(stream, MagicMock(), ["projects"], catalog)
        stream.write_schema.assert_called_once()

    def test_write_schema_skips_unselected_stream(self):
        from tap_gitlab.sync import write_schema
        stream = _make_stream_obj("projects", children=[])
        stream.is_selected.return_value = False

        catalog = MagicMock()
        write_schema(stream, MagicMock(), ["projects"], catalog)
        stream.write_schema.assert_not_called()

    def test_write_schema_adds_selected_child_to_child_to_sync(self):
        from tap_gitlab.sync import write_schema
        from tap_gitlab.streams import STREAMS

        client = MagicMock()
        client.config = {"start_date": "2023-01-01T00:00:00Z"}
        client.base_url = "https://gitlab.com/api/v4"

        parent_stream = _make_stream_obj("projects", children=["branches"])
        parent_stream.is_selected.return_value = True

        child_catalog_entry = MagicMock()
        child_catalog_entry.schema.to_dict.return_value = {}
        child_catalog_entry.metadata = []

        catalog = MagicMock()
        catalog.get_stream.return_value = child_catalog_entry
        streams_to_sync = ["projects", "branches"]

        write_schema(parent_stream, client, streams_to_sync, catalog)
        self.assertEqual(len(parent_stream.child_to_sync), 1)

    def test_write_schema_does_not_add_child_when_not_in_streams_to_sync(self):
        from tap_gitlab.sync import write_schema
        client = MagicMock()
        client.config = {"start_date": "2023-01-01T00:00:00Z"}
        client.base_url = "https://gitlab.com/api/v4"

        parent_stream = _make_stream_obj("projects", children=["branches"])
        parent_stream.is_selected.return_value = True

        child_catalog_entry = MagicMock()
        child_catalog_entry.schema.to_dict.return_value = {}
        child_catalog_entry.metadata = []

        catalog = MagicMock()
        catalog.get_stream.return_value = child_catalog_entry
        streams_to_sync = ["projects"]   # "branches" NOT in list

        write_schema(parent_stream, client, streams_to_sync, catalog)
        self.assertEqual(len(parent_stream.child_to_sync), 0)


class TestSync(unittest.TestCase):

    def test_sync_calls_stream_sync_for_top_level_stream(self):
        from tap_gitlab.sync import sync
        from tap_gitlab.streams import STREAMS

        client = MagicMock()
        client.config = {"start_date": "2023-01-01T00:00:00Z"}
        client.base_url = "https://gitlab.com/api/v4"

        projects_entry = MagicMock()
        projects_entry.tap_stream_id = "projects"
        projects_entry.stream = "projects"

        catalog = MagicMock(spec=Catalog)
        catalog.get_selected_streams.return_value = [projects_entry]

        mock_stream = _make_stream_obj("projects", parent="", children=[])
        mock_stream.sync.return_value = 3

        mock_projects_catalog_entry = MagicMock()
        mock_projects_catalog_entry.schema.to_dict.return_value = {}
        mock_projects_catalog_entry.metadata = []
        catalog.get_stream.return_value = mock_projects_catalog_entry

        mock_stream_cls = MagicMock(return_value=mock_stream)
        mock_streams = {"projects": mock_stream_cls}

        with patch("tap_gitlab.sync.STREAMS", mock_streams), \
             patch("tap_gitlab.sync.write_schema"), \
             patch("tap_gitlab.sync.update_currently_syncing"), \
             patch("singer.Transformer") as mock_tf:
            mock_tf.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tf.return_value.__exit__ = MagicMock(return_value=False)
            sync(client=client, config=client.config, catalog=catalog, state={})

        mock_stream.sync.assert_called_once()

    def test_sync_skips_child_stream_and_adds_parent(self):
        from tap_gitlab.sync import sync

        client = MagicMock()
        client.config = {"start_date": "2023-01-01T00:00:00Z"}
        client.base_url = "https://gitlab.com/api/v4"

        branches_entry = MagicMock()
        branches_entry.tap_stream_id = "branches"
        branches_entry.stream = "branches"

        catalog = MagicMock(spec=Catalog)
        catalog.get_selected_streams.return_value = [branches_entry]

        mock_child_stream = _make_stream_obj("branches", parent="projects", children=[])
        mock_parent_stream = _make_stream_obj("projects", parent="", children=["branches"])

        mock_catalog_entry = MagicMock()
        mock_catalog_entry.schema.to_dict.return_value = {}
        mock_catalog_entry.metadata = []
        catalog.get_stream.return_value = mock_catalog_entry

        mock_branches_cls = MagicMock(return_value=mock_child_stream)
        mock_projects_cls = MagicMock(return_value=mock_parent_stream)
        mock_streams = {"branches": mock_branches_cls, "projects": mock_projects_cls}

        with patch("tap_gitlab.sync.STREAMS", mock_streams), \
             patch("tap_gitlab.sync.write_schema"), \
             patch("tap_gitlab.sync.update_currently_syncing"), \
             patch("singer.Transformer") as mock_tf:
            mock_tf.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tf.return_value.__exit__ = MagicMock(return_value=False)
            sync(client=client, config=client.config, catalog=catalog, state={})

        # The child stream should NOT be synced directly
        mock_child_stream.sync.assert_not_called()
        # The parent stream SHOULD be synced
        mock_parent_stream.sync.assert_called_once()
