import unittest
from unittest.mock import patch, MagicMock
from tap_gitlab.discover import discover, _apply_access_checks, _prune_inaccessible_children
from tap_gitlab.exceptions import ForbiddenError


class TestAccessChecks(unittest.TestCase):
    """Tests for stream access check logic during discovery."""

    @patch("tap_gitlab.discover._prune_inaccessible_children")
    @patch("tap_gitlab.discover.STREAMS")
    def test_all_streams_accessible(self, mock_streams, mock_prune):
        """All streams accessible - none excluded."""
        mock_client = MagicMock()

        mock_stream_instance = MagicMock()
        mock_stream_instance.check_access.return_value = True
        mock_stream_cls = MagicMock(return_value=mock_stream_instance)

        mock_streams.items.return_value = [("projects", mock_stream_cls), ("groups", mock_stream_cls)]

        schemas = {"projects": {"properties": {}}, "groups": {"properties": {}}}
        field_metadata = {"projects": [], "groups": []}

        _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertIn("projects", schemas)
        self.assertIn("groups", schemas)

    @patch("tap_gitlab.discover._prune_inaccessible_children")
    @patch("tap_gitlab.discover.STREAMS")
    def test_partial_access(self, mock_streams, mock_prune):
        """Some streams inaccessible - those are excluded."""
        mock_client = MagicMock()

        accessible_instance = MagicMock()
        accessible_instance.check_access.return_value = True
        accessible_cls = MagicMock(return_value=accessible_instance)

        forbidden_instance = MagicMock()
        forbidden_instance.check_access.return_value = False
        forbidden_cls = MagicMock(return_value=forbidden_instance)

        mock_streams.items.return_value = [
            ("projects", accessible_cls),
            ("groups", forbidden_cls),
        ]

        schemas = {"projects": {"properties": {}}, "groups": {"properties": {}}}
        field_metadata = {"projects": [], "groups": []}

        _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertIn("projects", schemas)
        self.assertNotIn("groups", schemas)
        self.assertIn("projects", field_metadata)
        self.assertNotIn("groups", field_metadata)

    @patch("tap_gitlab.discover._prune_inaccessible_children")
    @patch("tap_gitlab.discover.STREAMS")
    def test_no_streams_accessible_raises(self, mock_streams, mock_prune):
        """All streams inaccessible - raises ForbiddenError."""
        mock_client = MagicMock()

        forbidden_instance = MagicMock()
        forbidden_instance.check_access.return_value = False
        forbidden_cls = MagicMock(return_value=forbidden_instance)

        mock_streams.items.return_value = [
            ("projects", forbidden_cls),
            ("groups", forbidden_cls),
        ]

        schemas = {"projects": {"properties": {}}, "groups": {"properties": {}}}
        field_metadata = {"projects": [], "groups": []}

        with self.assertRaises(ForbiddenError):
            _apply_access_checks(mock_client, schemas, field_metadata)

    @patch("tap_gitlab.discover._prune_inaccessible_children")
    @patch("tap_gitlab.discover.STREAMS")
    def test_no_streams_accessible_raises_with_message(self, mock_streams, mock_prune):
        """All streams inaccessible - ForbiddenError has correct message."""
        mock_client = MagicMock()

        forbidden_instance = MagicMock()
        forbidden_instance.check_access.return_value = False
        forbidden_cls = MagicMock(return_value=forbidden_instance)

        mock_streams.items.return_value = [
            ("projects", forbidden_cls),
            ("groups", forbidden_cls),
        ]

        schemas = {"projects": {"properties": {}}, "groups": {"properties": {}}}
        field_metadata = {"projects": [], "groups": []}

        with self.assertRaises(ForbiddenError) as context:
            _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertIn(
            "No streams are accessible. Ensure the credentials have read permission for at least one stream.",
            str(context.exception),
        )

    @patch("tap_gitlab.discover.LOGGER")
    @patch("tap_gitlab.discover._prune_inaccessible_children")
    @patch("tap_gitlab.discover.STREAMS")
    def test_partial_access_logs_warning(self, mock_streams, mock_prune, mock_logger):
        """Some streams inaccessible - logs warning listing excluded streams."""
        mock_client = MagicMock()

        accessible_instance = MagicMock()
        accessible_instance.check_access.return_value = True
        accessible_cls = MagicMock(return_value=accessible_instance)

        forbidden_instance = MagicMock()
        forbidden_instance.check_access.return_value = False
        forbidden_cls = MagicMock(return_value=forbidden_instance)

        mock_streams.items.return_value = [
            ("projects", accessible_cls),
            ("groups", forbidden_cls),
        ]

        schemas = {"projects": {"properties": {}}, "groups": {"properties": {}}}
        field_metadata = {"projects": [], "groups": []}

        _apply_access_checks(mock_client, schemas, field_metadata)

        mock_logger.warning.assert_called_with(
            "These streams have been excluded due to HTTP-Error-Code:403 Forbidden: %s",
            "groups",
        )

    def test_prune_inaccessible_children(self):
        """Child streams are removed when parent is excluded."""
        schemas = {
            "branches": {"properties": {}},
            "commits": {"properties": {}},
            "groups": {"properties": {}},
            "group_milestones": {"properties": {}},
        }
        field_metadata = {
            "branches": [],
            "commits": [],
            "groups": [],
            "group_milestones": [],
        }

        # branches and commits have parent="projects", which is not in schemas
        # group_milestones has parent="groups", which IS in schemas
        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("branches", schemas)
        self.assertNotIn("commits", schemas)
        self.assertIn("groups", schemas)
        self.assertIn("group_milestones", schemas)


class TestCheckAccessMethod(unittest.TestCase):
    """Tests for BaseStream.check_access()."""

    def test_child_stream_always_returns_true(self):
        """Child streams always return True without making API call."""
        from tap_gitlab.streams.branches import Branches

        mock_client = MagicMock()
        stream = Branches(client=mock_client)

        result = stream.check_access()

        self.assertTrue(result)
        mock_client.get.assert_not_called()

    def test_parent_stream_accessible(self):
        """Parent stream returns True when API call succeeds."""
        from tap_gitlab.streams.groups import Groups

        mock_client = MagicMock()
        mock_client.base_url = "https://gitlab.com/api/v4"
        mock_client.get.return_value = [{"id": 1}]
        stream = Groups(client=mock_client)

        result = stream.check_access()

        self.assertTrue(result)

    def test_parent_stream_forbidden(self):
        """Parent stream returns False when 403 is raised."""
        from tap_gitlab.streams.groups import Groups

        mock_client = MagicMock()
        mock_client.base_url = "https://gitlab.com/api/v4"
        mock_client.get.side_effect = ForbiddenError("Forbidden")
        stream = Groups(client=mock_client)

        result = stream.check_access()

        self.assertFalse(result)

    def test_parent_stream_forbidden_logs_warning(self):
        """Parent stream logs warning with stream name and error message on 403."""
        from tap_gitlab.streams.groups import Groups

        mock_client = MagicMock()
        mock_client.base_url = "https://gitlab.com/api/v4"
        mock_client.get.side_effect = ForbiddenError("403 Access Denied")
        stream = Groups(client=mock_client)

        with patch("tap_gitlab.streams.abstracts.LOGGER") as mock_logger:
            result = stream.check_access()

            self.assertFalse(result)
            mock_logger.warning.assert_called_once_with(
                "Unauthorized Stream: %s, excluding from catalog. HTTP-Error-Message:'%s'",
                stream.tap_stream_id,
                "403 Access Denied",
            )


class TestDiscoverWithClient(unittest.TestCase):
    """Tests for the discover() function accepting a client."""

    @patch("tap_gitlab.discover._apply_access_checks")
    @patch("tap_gitlab.discover.get_schemas")
    def test_discover_calls_access_checks(self, mock_get_schemas, mock_access_checks):
        """discover() calls _apply_access_checks with client."""
        mock_client = MagicMock()
        mock_get_schemas.return_value = ({}, {})

        discover(mock_client)

        mock_access_checks.assert_called_once()
        args = mock_access_checks.call_args[0]
        self.assertEqual(args[0], mock_client)


if __name__ == "__main__":
    unittest.main()
