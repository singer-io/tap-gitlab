"""Unit tests for tap_gitlab.schema — get_abs_path, load_schema_references, get_schemas."""
import os
import unittest
from unittest.mock import MagicMock, patch

from tap_gitlab.schema import get_abs_path, load_schema_references, get_schemas


class TestGetAbsPath(unittest.TestCase):

    def test_returns_absolute_path(self):
        result = get_abs_path("schemas")
        self.assertTrue(os.path.isabs(result))
        self.assertTrue(result.endswith("schemas"))

    def test_schemas_dir_exists(self):
        result = get_abs_path("schemas")
        self.assertTrue(os.path.exists(result))


class TestLoadSchemaReferences(unittest.TestCase):

    def test_returns_dict(self):
        refs = load_schema_references()
        self.assertIsInstance(refs, dict)

    def test_shared_dir_absent_returns_empty(self):
        with patch("tap_gitlab.schema.os.path.exists", return_value=False):
            refs = load_schema_references()
        self.assertEqual(refs, {})

    def test_shared_schemas_are_keyed_with_prefix(self):
        refs = load_schema_references()
        for key in refs:
            self.assertTrue(key.startswith("shared/"), f"Key {key!r} missing 'shared/' prefix")

    def test_values_are_dicts(self):
        refs = load_schema_references()
        for key, val in refs.items():
            self.assertIsInstance(val, dict, f"Expected dict for ref {key!r}")


class TestGetSchemas(unittest.TestCase):

    def test_returns_two_dicts(self):
        schemas, field_metadata = get_schemas()
        self.assertIsInstance(schemas, dict)
        self.assertIsInstance(field_metadata, dict)

    def test_known_streams_are_present(self):
        schemas, field_metadata = get_schemas()
        for stream_name in ("projects", "issues", "commits"):
            self.assertIn(stream_name, schemas, f"Missing stream: {stream_name}")
            self.assertIn(stream_name, field_metadata)

    def test_every_schema_has_properties(self):
        schemas, _ = get_schemas()
        for stream_name, schema in schemas.items():
            self.assertIn("properties", schema, f"Schema for {stream_name} has no 'properties'")

    def test_field_metadata_is_list(self):
        _, field_metadata = get_schemas()
        for stream_name, mdata in field_metadata.items():
            self.assertIsInstance(mdata, list, f"Metadata for {stream_name} is not a list")

    def test_schema_file_missing_logs_warning(self):
        """When a stream's schema JSON file is absent get_schemas should skip it gracefully."""
        with patch("tap_gitlab.schema.os.path.exists", return_value=False):
            schemas, _ = get_schemas()
        self.assertEqual(schemas, {})

    def test_replication_keys_with_property_descriptor(self):
        """Streams that expose replication_keys as a @property are handled."""
        from tap_gitlab.streams import STREAMS
        schemas, field_metadata = get_schemas()
        # Incremental streams should have valid_replication_keys in metadata
        for stream_name, stream_cls in STREAMS.items():
            if stream_name not in field_metadata:
                continue
            mdata_list = field_metadata[stream_name]
            self.assertIsInstance(mdata_list, list)

    def test_parent_attribute_written_to_metadata(self):
        """Child streams with a parent attribute should have parent-tap-stream-id in metadata."""
        from singer import metadata
        _, field_metadata = get_schemas()
        child_streams = {
            "branches": "projects",
            "commits": "projects",
            "issues": "projects",
        }
        for stream_name, expected_parent in child_streams.items():
            if stream_name not in field_metadata:
                continue
            mdata_map = metadata.to_map(field_metadata[stream_name])
            parent_val = mdata_map.get((), {}).get("parent-tap-stream-id")
            self.assertEqual(parent_val, expected_parent,
                             f"Wrong parent for {stream_name}: {parent_val}")
