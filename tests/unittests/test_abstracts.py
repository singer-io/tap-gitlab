"""Unit tests for tap_gitlab.streams.abstracts — BaseStream, IncrementalStream,
FullTableStream, ParentBaseStream, ChildBaseStream."""
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from singer import Transformer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_client(start_date="2023-01-01T00:00:00Z", config=None):
    client = MagicMock()
    client.base_url = "https://gitlab.com/api/v4"
    client.config = config or {"start_date": start_date}
    return client


def _mock_catalog(schema=None, mdata=None):
    entry = MagicMock()
    entry.schema.to_dict.return_value = schema or {"properties": {}}
    entry.metadata = mdata or []
    return entry


# ---------------------------------------------------------------------------
# Concrete implementations for testing abstract classes
# ---------------------------------------------------------------------------

def _make_incremental_stream(client=None, catalog=None):
    """Return a concrete IncrementalStream-like instance (Projects)."""
    from tap_gitlab.streams.projects import Projects
    c = client or _mock_client()
    cat = catalog or _mock_catalog()
    return Projects(client=c, catalog=cat)


def _make_full_table_stream(client=None, catalog=None):
    """Return a concrete FullTableStream-like instance (Groups)."""
    from tap_gitlab.streams.groups import Groups
    c = client or _mock_client(config={"start_date": "2023-01-01T00:00:00Z", "groups": "100"})
    cat = catalog or _mock_catalog()
    return Groups(client=c, catalog=cat)


def _make_child_stream(client=None, catalog=None):
    """Return a concrete ChildBaseStream-like instance (Commits)."""
    from tap_gitlab.streams.commits import Commits
    c = client or _mock_client()
    cat = catalog or _mock_catalog()
    return Commits(client=c, catalog=cat)


# ---------------------------------------------------------------------------
# BaseStream tests
# ---------------------------------------------------------------------------

class TestBaseStreamInit(unittest.TestCase):

    def test_schema_set_from_catalog(self):
        stream = _make_incremental_stream()
        self.assertIsInstance(stream.schema, dict)

    def test_child_to_sync_is_empty_list(self):
        stream = _make_incremental_stream()
        self.assertEqual(stream.child_to_sync, [])

    def test_is_selected_returns_none_for_empty_metadata(self):
        stream = _make_incremental_stream()
        # Empty metadata → None (falsy)
        self.assertFalse(stream.is_selected())

    def test_write_schema_calls_singer_write_schema(self):
        from tap_gitlab.streams.abstracts import BaseStream
        stream = _make_incremental_stream()
        stream.schema = {"properties": {"id": {"type": "integer"}}}
        with patch("tap_gitlab.streams.abstracts.write_schema") as mock_ws:
            stream.write_schema()
            mock_ws.assert_called_once_with(stream.tap_stream_id, stream.schema, stream.key_properties)

    def test_update_params_merges_kwargs(self):
        stream = _make_incremental_stream()
        stream.update_params(updated_after="2023-01-01")
        self.assertEqual(stream.params.get("updated_after"), "2023-01-01")

    def test_modify_object_returns_record_unchanged(self):
        stream = _make_incremental_stream()
        record = {"id": 1, "name": "test"}
        result = stream.modify_object(record)
        self.assertEqual(result, record)

    def test_get_url_endpoint_returns_url_endpoint_when_set(self):
        stream = _make_incremental_stream()
        stream.url_endpoint = "https://gitlab.com/api/v4/projects"
        result = stream.get_url_endpoint()
        self.assertEqual(result, "https://gitlab.com/api/v4/projects")


# ---------------------------------------------------------------------------
# BaseStream.get_records tests
# ---------------------------------------------------------------------------

class TestGetRecords(unittest.TestCase):
    """Tests against Groups which inherits BaseStream.get_records unchanged."""

    def test_get_records_yields_list_response(self):
        """BaseStream.get_records yields items from a list response."""
        from tap_gitlab.streams.group_milestones import GroupMilestones
        client = _mock_client(config={"start_date": "2023-01-01T00:00:00Z"})
        stream = GroupMilestones(client=client, catalog=_mock_catalog())
        stream.url_endpoint = "https://gitlab.com/api/v4/groups/1/milestones"
        records = [{"id": 1}, {"id": 2}]
        client.get.return_value = records
        client.last_response_headers = {}

        result = list(stream.get_records())
        self.assertEqual(result, records)

    def test_get_records_yields_data_key_response(self):
        from tap_gitlab.streams.group_milestones import GroupMilestones
        client = _mock_client(config={"start_date": "2023-01-01T00:00:00Z"})
        stream = GroupMilestones(client=client, catalog=_mock_catalog())
        stream.data_key = "data"
        stream.url_endpoint = "https://gitlab.com/api/v4/groups/1/milestones"
        client.get.return_value = {"data": [{"id": "abc"}]}
        client.last_response_headers = {}

        result = list(stream.get_records())
        self.assertEqual(result, [{"id": "abc"}])

    def test_get_records_paginates_with_x_next_page(self):
        from tap_gitlab.streams.group_milestones import GroupMilestones
        client = _mock_client(config={"start_date": "2023-01-01T00:00:00Z"})
        stream = GroupMilestones(client=client, catalog=_mock_catalog())
        stream.url_endpoint = "https://gitlab.com/api/v4/groups/1/milestones"
        page1 = [{"id": i} for i in range(100)]
        page2 = [{"id": 100}]
        client.get.side_effect = [page1, page2]

        call_count = 0

        def _get_headers():
            nonlocal call_count
            call_count += 1
            return {"x-next-page": "2"} if call_count == 1 else {}

        type(client).last_response_headers = property(lambda s: _get_headers())
        result = list(stream.get_records())
        self.assertEqual(len(result), 101)

    def test_get_records_stops_when_fewer_than_page_size(self):
        from tap_gitlab.streams.group_milestones import GroupMilestones
        client = _mock_client(config={"start_date": "2023-01-01T00:00:00Z"})
        stream = GroupMilestones(client=client, catalog=_mock_catalog())
        stream.url_endpoint = "https://gitlab.com/api/v4/groups/1/milestones"
        client.get.return_value = [{"id": 1}, {"id": 2}]  # < 100
        client.last_response_headers = {}

        result = list(stream.get_records())
        self.assertEqual(len(result), 2)
        client.get.assert_called_once()


# ---------------------------------------------------------------------------
# IncrementalStream tests
# ---------------------------------------------------------------------------

class TestIncrementalStreamGetBookmark(unittest.TestCase):

    def test_get_bookmark_returns_start_date_when_no_state(self):
        stream = _make_incremental_stream()
        bookmark = stream.get_bookmark({}, "projects")
        self.assertEqual(bookmark, "2023-01-01T00:00:00Z")

    def test_get_bookmark_returns_existing_bookmark(self):
        stream = _make_incremental_stream()
        # Set metadata so is_selected() returns True (ParentBaseStream.get_bookmark needs this)
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])
        state = {"bookmarks": {"projects": {"updated_at": "2023-06-01T00:00:00Z"}}}
        bookmark = stream.get_bookmark(state, "projects")
        self.assertEqual(bookmark, "2023-06-01T00:00:00Z")


class TestToUtcDatetime(unittest.TestCase):

    def test_none_returns_none(self):
        stream = _make_incremental_stream()
        self.assertIsNone(stream._to_utc_datetime(None))

    def test_datetime_with_tz(self):
        stream = _make_incremental_stream()
        dt = datetime(2023, 1, 1, tzinfo=timezone.utc)
        result = stream._to_utc_datetime(dt)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_naive_datetime_gets_utc(self):
        stream = _make_incremental_stream()
        dt = datetime(2023, 1, 1)  # naive
        result = stream._to_utc_datetime(dt)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_string_date_parsed(self):
        stream = _make_incremental_stream()
        result = stream._to_utc_datetime("2023-06-15T10:00:00Z")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2023)

    def test_unix_timestamp_parsed(self):
        stream = _make_incremental_stream()
        result = stream._to_utc_datetime(0)  # epoch
        self.assertIsInstance(result, datetime)

    def test_float_timestamp_parsed(self):
        stream = _make_incremental_stream()
        result = stream._to_utc_datetime(1_700_000_000.5)
        self.assertIsInstance(result, datetime)

    def test_unsupported_type_returns_none(self):
        stream = _make_incremental_stream()
        result = stream._to_utc_datetime(["not", "a", "date"])
        self.assertIsNone(result)


class TestUpdateBookmarkState(unittest.TestCase):

    def test_updates_state_with_new_bookmark(self):
        # Use Commits (ChildBaseStream → IncrementalStream) which calls update_bookmark_state directly
        stream = _make_child_stream()
        state = {}
        stream.update_bookmark_state(state, "commits", value="2023-08-01T00:00:00Z")
        self.assertEqual(
            state["bookmarks"]["commits"]["committed_date"], "2023-08-01T00:00:00Z"
        )

    def test_keeps_existing_bookmark_if_new_value_is_older(self):
        stream = _make_incremental_stream()
        state = {"bookmarks": {"projects": {"updated_at": "2023-09-01T00:00:00Z"}}}
        stream.update_bookmark_state(state, "projects", value="2023-06-01T00:00:00Z")
        # Should keep 2023-09 because it's later
        self.assertEqual(
            state["bookmarks"]["projects"]["updated_at"], "2023-09-01T00:00:00Z"
        )

    def test_no_replication_key_returns_state_unchanged(self):
        """IncrementalStream.update_bookmark_state returns early when no key/replication_keys."""
        from tap_gitlab.streams.abstracts import IncrementalStream
        mock_stream = MagicMock()
        mock_stream.replication_keys = []
        original_state = {}
        result = IncrementalStream.update_bookmark_state(mock_stream, original_state, "test_stream")
        self.assertEqual(result, {})


class TestIncrementalStreamSync(unittest.TestCase):

    def test_sync_writes_selected_records_after_bookmark(self):
        stream = _make_incremental_stream()
        # Mark as selected
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])
        stream.schema = {"properties": {"id": {"type": ["null", "integer"]},
                                        "updated_at": {"type": ["null", "string"]}}}

        records = [{"id": 1, "updated_at": "2023-06-01T00:00:00Z"},
                   {"id": 2, "updated_at": "2023-08-01T00:00:00Z"}]

        stream.get_records = MagicMock(return_value=iter(records))
        with patch("tap_gitlab.streams.abstracts.write_record") as mock_wr:
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                count = stream.sync(
                    state={"bookmarks": {"projects": {"updated_at": "2023-05-01T00:00:00Z"}}},
                    transformer=transformer
                )
        self.assertGreaterEqual(count, 1)

    def test_sync_skips_records_before_bookmark(self):
        stream = _make_incremental_stream()
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])
        stream.schema = {"properties": {"updated_at": {"type": ["null", "string"]}}}

        records = [{"id": 1, "updated_at": "2022-01-01T00:00:00Z"}]  # before bookmark
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record") as mock_wr:
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                count = stream.sync(
                    state={"bookmarks": {"projects": {"updated_at": "2023-01-01T00:00:00Z"}}},
                    transformer=transformer
                )
        mock_wr.assert_not_called()

    def test_sync_skips_record_with_invalid_replication_key(self):
        stream = _make_incremental_stream()
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])
        stream.schema = {"properties": {"updated_at": {"type": ["null", "string"]}}}

        records = [{"id": 1, "updated_at": None}]  # None → invalid
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record") as mock_wr:
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                count = stream.sync(state={}, transformer=transformer)
        mock_wr.assert_not_called()
        self.assertEqual(count, 0)

    def test_sync_not_selected_does_not_write_record(self):
        stream = _make_incremental_stream()
        # Use proper empty metadata map so is_selected() returns None without raising
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([])
        stream.schema = {"properties": {"updated_at": {"type": ["null", "string"]}}}

        records = [{"id": 1, "updated_at": "2023-06-01T00:00:00Z"}]
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record") as mock_wr:
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                stream.sync(state={}, transformer=transformer)
        mock_wr.assert_not_called()

    def test_sync_syncs_children(self):
        stream = _make_incremental_stream()
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])
        stream.schema = {"properties": {"updated_at": {"type": ["null", "string"]}}}

        child = MagicMock()
        child.sync.return_value = 2
        stream.child_to_sync = [child]

        records = [{"id": 1, "updated_at": "2023-06-01T00:00:00Z"}]
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record"):
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                stream.sync(
                    state={"bookmarks": {"projects": {"updated_at": "2023-01-01T00:00:00Z"}}},
                    transformer=transformer
                )
        child.sync.assert_called_once()


# ---------------------------------------------------------------------------
# FullTableStream tests
# ---------------------------------------------------------------------------

class TestFullTableStreamSync(unittest.TestCase):

    def test_sync_writes_all_records_when_selected(self):
        stream = _make_full_table_stream()
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])
        stream.schema = {"properties": {"id": {"type": ["null", "integer"]}}}
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record") as mock_wr:
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                count = stream.sync(state={}, transformer=transformer)
        self.assertEqual(count, 3)

    def test_sync_does_not_write_when_not_selected(self):
        stream = _make_full_table_stream()
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([])  # empty metadata → not selected
        stream.schema = {"properties": {"id": {"type": ["null", "integer"]}}}
        records = [{"id": 1}]
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record") as mock_wr:
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                count = stream.sync(state={}, transformer=transformer)
        mock_wr.assert_not_called()
        self.assertEqual(count, 0)

    def test_sync_calls_child_sync(self):
        stream = _make_full_table_stream()
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])
        stream.schema = {"properties": {"id": {"type": ["null", "integer"]}}}
        child = MagicMock()
        child.sync.return_value = 0
        stream.child_to_sync = [child]
        records = [{"id": 1}]
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record"):
            from singer import Transformer as RealTransformer
            with RealTransformer() as transformer:
                stream.sync(state={}, transformer=transformer)
        child.sync.assert_called_once_with(state={}, transformer=transformer, parent_obj={"id": 1})


# ---------------------------------------------------------------------------
# ChildBaseStream tests
# ---------------------------------------------------------------------------

class TestChildBaseStreamGetBookmark(unittest.TestCase):

    def test_get_bookmark_cached_on_second_call(self):
        stream = _make_child_stream()
        stream.bookmark_value = None
        state = {}
        # First call — should read from state/start_date
        b1 = stream.get_bookmark(state, "commits")
        # Second call — returns cached value
        b2 = stream.get_bookmark(state, "commits")
        self.assertEqual(b1, b2)

    def test_get_bookmark_uses_cached_value(self):
        stream = _make_child_stream()
        stream.bookmark_value = "2023-05-01T00:00:00Z"
        result = stream.get_bookmark({}, "commits")
        self.assertEqual(result, "2023-05-01T00:00:00Z")


class TestChildBaseStreamGetUrl(unittest.TestCase):

    def test_get_url_raises_when_no_parent_obj(self):
        stream = _make_child_stream()
        with self.assertRaises(ValueError):
            stream.get_url(None)

    def test_get_url_raises_when_missing_id_in_parent(self):
        stream = _make_child_stream()
        with self.assertRaises(ValueError):
            stream.get_url({"name": "no-id"})

    def test_get_url_returns_formatted_path(self):
        stream = _make_child_stream()
        url = stream.get_url({"id": 42})
        self.assertIn("42", url)

    def test_get_url_endpoint_builds_full_url(self):
        stream = _make_child_stream()
        endpoint = stream.get_url_endpoint({"id": 100})
        self.assertIn("https://gitlab.com/api/v4", endpoint)
        self.assertIn("100", endpoint)


# ---------------------------------------------------------------------------
# ParentBaseStream tests
# ---------------------------------------------------------------------------

class TestParentBaseStream(unittest.TestCase):

    def _make_parent_stream(self):
        from tap_gitlab.streams.projects import Projects
        client = _mock_client()
        cat = _mock_catalog()
        return Projects(client=client, catalog=cat)

    def test_get_bookmark_returns_start_date_when_no_children(self):
        stream = self._make_parent_stream()
        stream.child_to_sync = []
        # Projects is a ParentBaseStream; with no children and no state → start_date
        result = stream.get_bookmark({}, "projects")
        self.assertEqual(result, "2023-01-01T00:00:00Z")

    def test_update_bookmark_state_writes_to_children(self):
        stream = self._make_parent_stream()
        from singer import metadata as singer_metadata
        stream.metadata = singer_metadata.to_map([
            {"breadcrumb": [], "metadata": {"selected": True}}
        ])

        child = MagicMock()
        child.tap_stream_id = "branches"

        # Simulate IncrementalStream.update_bookmark_state behaviour
        from tap_gitlab.streams.abstracts import IncrementalStream
        child.__class__ = MagicMock
        stream.child_to_sync = [child]

        state = {}
        new_value = "2023-08-01T00:00:00Z"

        # Should not raise
        stream.update_bookmark_state(state, "projects", value=new_value)
