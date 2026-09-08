"""Additional targeted tests to cover remaining source-code gaps."""
import json
import unittest
from unittest.mock import MagicMock, patch, mock_open

import requests
from singer import Transformer as RealTransformer


# ---------------------------------------------------------------------------
# tap_gitlab.discover — except block in discover()
# ---------------------------------------------------------------------------

class TestDiscoverExceptBlock(unittest.TestCase):

    def test_discover_logs_and_reraises_when_schema_from_dict_fails(self):
        """The except block (lines 20-24) in discover() is hit when Schema.from_dict raises."""
        from singer.catalog import Schema

        good_schemas = {"projects": {"type": "object", "properties": {}}}
        good_mdata = {"projects": []}

        with patch("tap_gitlab.discover.get_schemas", return_value=(good_schemas, good_mdata)), \
             patch.object(Schema, "from_dict", side_effect=Exception("bad schema")):
            from tap_gitlab.discover import discover as _discover
            with self.assertRaises(Exception):
                _discover()


# ---------------------------------------------------------------------------
# tap_gitlab.schema — shared refs loading and exception paths
# ---------------------------------------------------------------------------

class TestSchemaSharedRefs(unittest.TestCase):

    def test_load_schema_references_reads_files_when_shared_dir_exists(self):
        """Lines 22, 29-30: When shared dir exists with JSON files, they are loaded."""
        from tap_gitlab.schema import load_schema_references
        fake_json = json.dumps({"type": "string"})

        with patch("tap_gitlab.schema.os.path.exists", return_value=True), \
             patch("tap_gitlab.schema.os.listdir", return_value=["shard.json"]), \
             patch("tap_gitlab.schema.os.path.isfile", return_value=True), \
             patch("builtins.open", mock_open(read_data=fake_json)):
            refs = load_schema_references()

        self.assertIn("shared/shard.json", refs)
        self.assertEqual(refs["shared/shard.json"], {"type": "string"})


class TestGetSchemasExceptionPaths(unittest.TestCase):

    def test_get_schemas_handles_replication_keys_property_exception(self):
        """Lines 72-74: If resolving replication_keys raises, it falls back to []."""
        import tap_gitlab.schema as schema_module

        class BrokenReplicationKeyStream:
            """Stream class where replication_keys is a @property that raises."""
            tap_stream_id = "broken"
            replication_method = "INCREMENTAL"
            key_properties = ["id"]
            parent = None

            @property
            def replication_keys(self):
                raise RuntimeError("intentional error")

        broken_instance = BrokenReplicationKeyStream()
        fake_streams_map = {"broken": MagicMock(return_value=broken_instance)}

        fake_schema = {"type": "object", "properties": {"id": {"type": "integer"}}}

        with patch.object(schema_module, "STREAMS", fake_streams_map), \
             patch("tap_gitlab.schema.os.path.exists", return_value=True), \
             patch("tap_gitlab.schema.os.listdir", return_value=[]), \
             patch("builtins.open", mock_open(read_data=json.dumps(fake_schema))):
            schemas, mdata = schema_module.get_schemas()

        self.assertIn("broken", schemas)

    def test_get_schemas_raises_when_get_standard_metadata_missing(self):
        """Lines 86-88: AttributeError from metadata.get_standard_metadata is re-raised."""
        import tap_gitlab.schema as schema_module
        from singer import metadata as singer_metadata

        fake_schema = {"type": "object", "properties": {"id": {"type": "integer"}}}

        with patch.object(schema_module, "STREAMS", {"projects": MagicMock()}), \
             patch("tap_gitlab.schema.os.path.exists", return_value=True), \
             patch("tap_gitlab.schema.os.listdir", return_value=[]), \
             patch("builtins.open", mock_open(read_data=json.dumps(fake_schema))), \
             patch.object(singer_metadata, "get_standard_metadata",
                          side_effect=AttributeError("not found")):
            with self.assertRaises(AttributeError):
                schema_module.get_schemas()


# ---------------------------------------------------------------------------
# tap_gitlab.client — post() method (lines 152-154)
# ---------------------------------------------------------------------------

class TestClientPost(unittest.TestCase):

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_post_returns_json_response(self, mock_creds):
        from tap_gitlab.client import Client
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)

        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"id": 99}
        mock_resp.headers = {}

        with patch.object(client._session, "request", return_value=mock_resp):
            result = client.post(
                endpoint="https://gitlab.com/api/v4/projects",
                params={},
                headers={},
                body={"name": "test"}
            )
        self.assertEqual(result, {"id": 99})

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_post_uses_path_when_no_endpoint(self, mock_creds):
        from tap_gitlab.client import Client
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)

        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"id": 5}
        mock_resp.headers = {}

        with patch.object(client._session, "request", return_value=mock_resp) as mock_req:
            result = client.post(
                endpoint="",
                params={},
                headers={},
                body={"title": "test"},
                path="projects"
            )
        # Verify the URL used includes the base URL + path
        call_url = mock_req.call_args[0][1]
        self.assertIn("projects", call_url)


# ---------------------------------------------------------------------------
# tap_gitlab.streams.abstracts — remaining gaps
# ---------------------------------------------------------------------------

def _mock_client(start_date="2023-01-01T00:00:00Z", config=None):
    client = MagicMock()
    client.base_url = "https://gitlab.com/api/v4"
    client.config = config or {"start_date": start_date}
    return client


def _mock_catalog(schema=None):
    entry = MagicMock()
    entry.schema.to_dict.return_value = schema or {"properties": {}}
    entry.metadata = []
    return entry


class TestGetRecordsPagination(unittest.TestCase):

    def test_get_records_tries_next_page_when_full_page(self):
        """Line 105: current_page += 1 when response is exactly page_size (no x-next-page)."""
        from tap_gitlab.streams.group_milestones import GroupMilestones
        client = _mock_client(config={"start_date": "2023-01-01T00:00:00Z"})
        stream = GroupMilestones(client=client, catalog=_mock_catalog())
        stream.url_endpoint = "https://gitlab.com/api/v4/groups/1/milestones"
        stream.page_size = 3  # small page size for testing

        full_page = [{"id": 1}, {"id": 2}, {"id": 3}]  # == page_size
        empty_page = []

        client.get.side_effect = [full_page, empty_page]
        client.last_response_headers = {}

        result = list(stream.get_records())
        self.assertEqual(len(result), 3)
        self.assertEqual(client.get.call_count, 2)


class TestGetUrlEndpointFallback(unittest.TestCase):

    def test_get_url_endpoint_uses_base_url_and_path_when_no_url_endpoint(self):
        """Line 117: BaseStream.get_url_endpoint fallback to base_url/path when url_endpoint is empty."""
        from tap_gitlab.streams.groups import Groups
        client = _mock_client(config={"start_date": "2023-01-01T00:00:00Z", "groups": "1"})
        stream = Groups(client=client, catalog=_mock_catalog())
        # Groups inherits BaseStream.get_url_endpoint (not overridden at base level)
        # Directly call the BaseStream version
        from tap_gitlab.streams.abstracts import BaseStream
        stream.url_endpoint = ""  # empty
        stream.path = "groups/1/milestones"

        endpoint = BaseStream.get_url_endpoint(stream)
        self.assertIn("https://gitlab.com/api/v4", endpoint)
        self.assertIn("groups/1/milestones", endpoint)


class TestUpdateBookmarkStateTypeError(unittest.TestCase):

    def test_update_bookmark_state_handles_comparison_error(self):
        """Lines 139-141: TypeError during max(current, new) falls back to current."""
        from tap_gitlab.streams.abstracts import IncrementalStream
        from tap_gitlab.streams.commits import Commits

        client = _mock_client()
        stream = Commits(client=client, catalog=_mock_catalog())

        # State has a valid bookmark; new value is incomparable (e.g. dict vs str)
        state = {"bookmarks": {"commits": {"committed_date": "2023-06-01T00:00:00Z"}}}
        # Pass an incomparable value type to trigger TypeError
        with patch("tap_gitlab.streams.abstracts.get_bookmark", return_value="2023-06-01T00:00:00Z"), \
             patch("builtins.max", side_effect=TypeError("incomparable")):
            result = stream.update_bookmark_state(state, "commits", value="not-a-date")

        # Should fall back to current bookmark and not raise
        self.assertIn("commits", result.get("bookmarks", {}))


class TestToUtcDatetimeNaiveString(unittest.TestCase):

    def test_naive_string_gets_utc_attached(self):
        """Line 163: a datetime string without timezone info gets UTC attached."""
        from tap_gitlab.streams.projects import Projects
        client = _mock_client()
        stream = Projects(client=client, catalog=_mock_catalog())

        result = stream._to_utc_datetime("2023-06-15T10:00:00")  # no timezone
        from datetime import timezone
        self.assertEqual(result.tzinfo, timezone.utc)


class TestIncrementalStreamSyncEdgeCases(unittest.TestCase):

    def _make_stream(self, selected=True):
        from tap_gitlab.streams.commits import Commits
        from singer import metadata as singer_metadata
        client = _mock_client()
        stream = Commits(client=client, catalog=_mock_catalog())
        if selected:
            stream.metadata = singer_metadata.to_map([
                {"breadcrumb": [], "metadata": {"selected": True}}
            ])
        else:
            stream.metadata = singer_metadata.to_map([])
        stream.schema = {"properties": {"committed_date": {"type": ["null", "string"]},
                                        "project_id": {"type": ["null", "integer"]}}}
        # Mock url_endpoint to bypass ChildBaseStream.get_url_endpoint parent requirement
        stream.url_endpoint = "https://gitlab.com/api/v4/projects/1/repository/commits"
        return stream

    def test_sync_uses_start_date_when_bookmark_is_invalid(self):
        """Lines 177-178: When bookmark resolves to None, start_date is used as fallback."""
        stream = self._make_stream(selected=False)
        from datetime import datetime, timezone
        fallback_dt = datetime(2023, 1, 1, tzinfo=timezone.utc)

        # First _to_utc_datetime call (bookmark) → None; second call (start_date fallback) → real dt
        with patch.object(stream, "_to_utc_datetime") as mock_utc, \
             patch.object(stream, "get_url_endpoint", return_value=stream.url_endpoint):
            mock_utc.side_effect = [None, fallback_dt]
            stream.get_records = MagicMock(return_value=iter([]))
            with RealTransformer() as transformer:
                count = stream.sync(state={}, transformer=transformer)
        self.assertEqual(count, 0)

    def test_sync_handles_timestamp_comparison_type_error(self):
        """Lines 204-205: TypeError during max(current, record_dt) is caught."""
        stream = self._make_stream(selected=True)
        from datetime import datetime, timezone

        bookmark_dt = datetime(2023, 1, 1, tzinfo=timezone.utc)
        record_dt = datetime(2023, 6, 1, tzinfo=timezone.utc)

        call_count = {"n": 0}
        import builtins
        real_max = builtins.max

        def patched_max(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise TypeError("can't compare")
            return real_max(*args, **kwargs)

        records = [{"id": "abc", "committed_date": "2023-06-01T00:00:00Z", "project_id": 1}]
        stream.get_records = MagicMock(return_value=iter(records))

        with patch("tap_gitlab.streams.abstracts.write_record"), \
             patch.object(stream, "_to_utc_datetime") as mock_utc, \
             patch.object(stream, "get_url_endpoint", return_value=stream.url_endpoint), \
             patch.object(stream, "modify_object", side_effect=lambda r, p=None: r), \
             patch("tap_gitlab.streams.abstracts.max", side_effect=patched_max):
            mock_utc.side_effect = [bookmark_dt, record_dt]
            with RealTransformer() as transformer:
                count = stream.sync(
                    state={"bookmarks": {"commits": {"committed_date": "2023-01-01T00:00:00Z"}}},
                    transformer=transformer
                )
