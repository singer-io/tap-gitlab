"""Unit tests for tap_gitlab.discover — discover()."""
import unittest
from unittest.mock import patch

from singer.catalog import Catalog


class TestDiscover(unittest.TestCase):

    def test_returns_catalog(self):
        from tap_gitlab.discover import discover
        catalog = discover()
        self.assertIsInstance(catalog, Catalog)

    def test_catalog_has_streams(self):
        from tap_gitlab.discover import discover
        catalog = discover()
        self.assertGreater(len(catalog.streams), 0)

    def test_all_streams_have_stream_name(self):
        from tap_gitlab.discover import discover
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.stream)

    def test_all_streams_have_key_properties(self):
        from tap_gitlab.discover import discover
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(
                entry.key_properties,
                f"Stream {entry.stream!r} has no key_properties",
            )

    def test_known_streams_in_catalog(self):
        from tap_gitlab.discover import discover
        catalog = discover()
        stream_names = {e.stream for e in catalog.streams}
        for name in ("projects", "issues", "commits", "branches"):
            self.assertIn(name, stream_names)

    def test_discover_raises_on_bad_schema(self):
        """When get_schemas raises, discover() must propagate the error."""
        with patch('tap_gitlab.discover.get_schemas', side_effect=RuntimeError("bad schema")):
            from tap_gitlab.discover import discover as _discover
            with self.assertRaises(RuntimeError):
                _discover()
