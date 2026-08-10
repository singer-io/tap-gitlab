"""Unit tests for concrete stream classes:
branches, commits, groups, issues, project_milestones, users."""
import unittest
from unittest.mock import MagicMock, patch


def _mock_client(config=None):
    client = MagicMock()
    client.base_url = "https://gitlab.com/api/v4"
    client.config = config or {"start_date": "2023-01-01T00:00:00Z", "groups": "100"}
    return client


def _mock_catalog():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {}
    entry.metadata = []
    return entry


# ---------------------------------------------------------------------------
# Branches
# ---------------------------------------------------------------------------

class TestBranches(unittest.TestCase):

    def _make(self):
        from tap_gitlab.streams.branches import Branches
        return Branches(client=_mock_client(), catalog=_mock_catalog())

    def test_get_url_raises_when_no_parent_obj(self):
        stream = self._make()
        with self.assertRaises(ValueError):
            stream.get_url(None)

    def test_get_url_raises_when_missing_id(self):
        stream = self._make()
        with self.assertRaises(ValueError):
            stream.get_url({"name": "main"})

    def test_get_url_includes_project_id(self):
        stream = self._make()
        url = stream.get_url({"id": 99})
        self.assertIn("99", url)
        self.assertIn("repository/branches", url)

    def test_get_url_endpoint_includes_base_url(self):
        stream = self._make()
        endpoint = stream.get_url_endpoint({"id": 7})
        self.assertIn("https://gitlab.com/api/v4", endpoint)

    def test_modify_object_adds_project_id_and_updated_at(self):
        stream = self._make()
        record = {"name": "main", "merged": False}
        parent = {"id": 42, "updated_at": "2023-05-01T00:00:00Z"}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 42)
        self.assertEqual(result["projects_updated_at"], "2023-05-01T00:00:00Z")

    def test_modify_object_handles_non_dict_record(self):
        stream = self._make()
        result = stream.modify_object("not-a-dict", {"id": 1})
        self.assertEqual(result, "not-a-dict")

    def test_modify_object_handles_none_parent(self):
        stream = self._make()
        record = {"name": "main"}
        result = stream.modify_object(record, None)
        self.assertNotIn("project_id", result)

    def test_tap_stream_id(self):
        stream = self._make()
        self.assertEqual(stream.tap_stream_id, "branches")

    def test_key_properties(self):
        stream = self._make()
        self.assertIn("project_id", stream.key_properties)
        self.assertIn("name", stream.key_properties)

    def test_parent(self):
        stream = self._make()
        self.assertEqual(stream.parent, "projects")


# ---------------------------------------------------------------------------
# Commits
# ---------------------------------------------------------------------------

class TestCommits(unittest.TestCase):

    def _make(self):
        from tap_gitlab.streams.commits import Commits
        return Commits(client=_mock_client(), catalog=_mock_catalog())

    def test_modify_object_adds_project_id(self):
        stream = self._make()
        record = {"id": "abc123", "committed_date": "2023-01-15T00:00:00Z"}
        parent = {"id": 55}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 55)

    def test_modify_object_handles_non_dict(self):
        stream = self._make()
        result = stream.modify_object("not-a-dict", {"id": 1})
        self.assertEqual(result, "not-a-dict")

    def test_tap_stream_id(self):
        stream = self._make()
        self.assertEqual(stream.tap_stream_id, "commits")

    def test_replication_key(self):
        stream = self._make()
        self.assertIn("committed_date", stream.replication_keys)


# ---------------------------------------------------------------------------
# Issues
# ---------------------------------------------------------------------------

class TestIssues(unittest.TestCase):

    def _make(self):
        from tap_gitlab.streams.issues import Issues
        return Issues(client=_mock_client(), catalog=_mock_catalog())

    def test_modify_object_adds_project_id(self):
        stream = self._make()
        record = {"id": 10, "title": "Bug"}
        parent = {"id": 200}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 200)

    def test_modify_object_handles_non_dict(self):
        stream = self._make()
        result = stream.modify_object(None, {"id": 1})
        self.assertIsNone(result)

    def test_tap_stream_id(self):
        stream = self._make()
        self.assertEqual(stream.tap_stream_id, "issues")

    def test_replication_key(self):
        stream = self._make()
        self.assertIn("updated_at", stream.replication_keys)


# ---------------------------------------------------------------------------
# ProjectMilestones
# ---------------------------------------------------------------------------

class TestProjectMilestones(unittest.TestCase):

    def _make(self):
        from tap_gitlab.streams.project_milestones import ProjectMilestones
        return ProjectMilestones(client=_mock_client(), catalog=_mock_catalog())

    def test_modify_object_adds_project_id_when_absent(self):
        stream = self._make()
        record = {"id": 5, "title": "Sprint 1"}
        parent = {"id": 99}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 99)

    def test_modify_object_does_not_overwrite_existing_project_id(self):
        stream = self._make()
        record = {"id": 5, "project_id": 77}
        parent = {"id": 99}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 77)

    def test_modify_object_handles_non_dict(self):
        stream = self._make()
        result = stream.modify_object("x", {"id": 1})
        self.assertEqual(result, "x")

    def test_tap_stream_id(self):
        stream = self._make()
        self.assertEqual(stream.tap_stream_id, "project_milestones")


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

class TestUsers(unittest.TestCase):

    def _make(self):
        from tap_gitlab.streams.users import Users
        return Users(client=_mock_client(), catalog=_mock_catalog())

    def test_get_url_raises_when_no_parent_obj(self):
        stream = self._make()
        with self.assertRaises(ValueError):
            stream.get_url(None)

    def test_get_url_raises_when_missing_id(self):
        stream = self._make()
        with self.assertRaises(ValueError):
            stream.get_url({"login": "user"})

    def test_get_url_includes_project_id(self):
        stream = self._make()
        url = stream.get_url({"id": 123})
        self.assertIn("123", url)
        self.assertIn("users", url)

    def test_get_url_endpoint_includes_base_url(self):
        stream = self._make()
        endpoint = stream.get_url_endpoint({"id": 10})
        self.assertIn("https://gitlab.com/api/v4", endpoint)

    def test_modify_object_adds_project_id_and_updated_at(self):
        stream = self._make()
        record = {"id": 1, "username": "alice"}
        parent = {"id": 50, "updated_at": "2023-07-01T00:00:00Z"}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 50)
        self.assertEqual(result["projects_updated_at"], "2023-07-01T00:00:00Z")

    def test_modify_object_handles_non_dict_record(self):
        stream = self._make()
        result = stream.modify_object(42, {"id": 1})
        self.assertEqual(result, 42)

    def test_tap_stream_id(self):
        stream = self._make()
        self.assertEqual(stream.tap_stream_id, "users")

    def test_parent(self):
        stream = self._make()
        self.assertEqual(stream.parent, "projects")


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

class TestGroups(unittest.TestCase):

    def _make(self, groups="100, 200"):
        from tap_gitlab.streams.groups import Groups
        config = {"start_date": "2023-01-01T00:00:00Z", "groups": groups}
        return Groups(client=_mock_client(config=config), catalog=_mock_catalog())

    def test_get_group_ids_comma_separated(self):
        stream = self._make("100,200,300")
        ids = stream.get_group_ids()
        self.assertEqual(ids, ["100", "200", "300"])

    def test_get_group_ids_space_separated(self):
        stream = self._make("100 200 300")
        ids = stream.get_group_ids()
        self.assertEqual(ids, ["100", "200", "300"])

    def test_get_group_ids_empty_returns_empty(self):
        stream = self._make("")
        ids = stream.get_group_ids()
        self.assertEqual(ids, [])

    def test_get_group_ids_no_groups_key(self):
        from tap_gitlab.streams.groups import Groups
        config = {"start_date": "2023-01-01T00:00:00Z"}
        stream = Groups(client=_mock_client(config=config), catalog=_mock_catalog())
        ids = stream.get_group_ids()
        self.assertEqual(ids, [])

    def test_get_url_endpoint_with_current_group_id(self):
        stream = self._make("42")
        stream._current_group_id = "42"
        endpoint = stream.get_url_endpoint()
        self.assertIn("42", endpoint)
        self.assertIn("groups", endpoint)

    def test_get_url_endpoint_without_current_group_id(self):
        stream = self._make("42")
        endpoint = stream.get_url_endpoint()
        self.assertIn("groups", endpoint)

    def test_get_records_yields_group_dicts(self):
        stream = self._make("100")
        stream.client.get.return_value = {"id": 100, "name": "my-group"}
        records = list(stream.get_records())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 100)

    def test_get_records_warns_on_unexpected_response_type(self):
        stream = self._make("100")
        stream.client.get.return_value = [{"id": 100}]  # list instead of dict
        # Should not raise; just logs a warning
        records = list(stream.get_records())
        self.assertEqual(records, [])

    def test_get_records_continues_after_exception(self):
        stream = self._make("100,200")
        stream.client.get.side_effect = [Exception("API error"), {"id": 200, "name": "ok"}]
        records = list(stream.get_records())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 200)

    def test_tap_stream_id(self):
        stream = self._make()
        self.assertEqual(stream.tap_stream_id, "groups")

    def test_children_include_group_milestones(self):
        stream = self._make()
        self.assertIn("group_milestones", stream.children)
