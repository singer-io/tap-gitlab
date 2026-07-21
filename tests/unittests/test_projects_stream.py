import unittest
from unittest.mock import MagicMock


def make_mock_client(config):
    """Create a minimal mock client with a base_url and config."""
    client = MagicMock()
    client.config = config
    client.base_url = "https://gitlab.com/api/v4"
    return client


def make_mock_catalog_entry():
    """Create a minimal mock catalog entry."""
    entry = MagicMock()
    entry.schema.to_dict.return_value = {}
    entry.metadata = []
    return entry


def make_projects_stream(config):
    from tap_gitlab.streams.projects import Projects
    client = make_mock_client(config)
    catalog_entry = make_mock_catalog_entry()
    return Projects(client=client, catalog=catalog_entry)


class TestGetGroupProjectIds(unittest.TestCase):
    """Tests for Projects.get_group_project_ids()"""

    def test_no_groups_in_config_returns_empty_set(self):
        stream = make_projects_stream({"projects": "123"})
        result = stream.get_group_project_ids()
        self.assertEqual(result, set())
        stream.client.get.assert_not_called()

    def test_empty_groups_string_returns_empty_set(self):
        stream = make_projects_stream({"groups": ""})
        result = stream.get_group_project_ids()
        self.assertEqual(result, set())
        stream.client.get.assert_not_called()

    def test_whitespace_only_groups_returns_empty_set(self):
        stream = make_projects_stream({"groups": "   "})
        result = stream.get_group_project_ids()
        self.assertEqual(result, set())
        stream.client.get.assert_not_called()

    def test_single_group_single_page(self):
        stream = make_projects_stream({"groups": "100"})
        stream.client.get.side_effect = [
            [{"id": 10}, {"id": 20}],  # page 1 — fewer than 100, stops
        ]
        result = stream.get_group_project_ids()
        self.assertEqual(result, {"10", "20"})
        stream.client.get.assert_called_once_with(
            "https://gitlab.com/api/v4/groups/100/projects",
            {"per_page": stream.page_size, "page": 1},
            stream.headers,
            None
        )

    def test_multiple_groups(self):
        stream = make_projects_stream({"groups": "100 200"})
        stream.client.get.side_effect = [
            [{"id": 10}],   # group 100, page 1
            [{"id": 20}],   # group 200, page 1
        ]
        result = stream.get_group_project_ids()
        self.assertEqual(result, {"10", "20"})

    def test_groups_comma_separated(self):
        stream = make_projects_stream({"groups": "100, 200, 300"})
        stream.client.get.side_effect = [
            [{"id": 1}],
            [{"id": 2}],
            [{"id": 3}],
        ]
        result = stream.get_group_project_ids()
        self.assertEqual(result, {"1", "2", "3"})

    def test_pagination_fetches_all_pages(self):
        stream = make_projects_stream({"groups": "100"})
        page_size = stream.page_size
        # First page returns a full page (triggers next page), second returns fewer
        page1 = [{"id": i} for i in range(1, page_size + 1)]
        page2 = [{"id": i} for i in range(page_size + 1, page_size + 6)]  # 5 items — stops
        stream.client.get.side_effect = [page1, page2]

        result = stream.get_group_project_ids()
        self.assertEqual(len(result), page_size + 5)
        self.assertEqual(stream.client.get.call_count, 2)

    def test_empty_response_stops_pagination(self):
        stream = make_projects_stream({"groups": "100"})
        stream.client.get.return_value = []
        result = stream.get_group_project_ids()
        self.assertEqual(result, set())
        stream.client.get.assert_called_once()

    def test_non_list_response_stops_pagination(self):
        stream = make_projects_stream({"groups": "100"})
        stream.client.get.return_value = {"error": "something"}
        result = stream.get_group_project_ids()
        self.assertEqual(result, set())

    def test_group_id_is_url_encoded(self):
        stream = make_projects_stream({"groups": "my/group"})
        stream.client.get.return_value = []
        stream.get_group_project_ids()
        called_endpoint = stream.client.get.call_args[0][0]
        self.assertIn("my%2Fgroup", called_endpoint)
        self.assertNotIn("my/group", called_endpoint.replace("https://gitlab.com/api/v4/groups/", ""))


class TestGetProjectIds(unittest.TestCase):
    """Tests for Projects.get_project_ids() — merged config + group IDs"""

    def test_config_projects_only(self):
        stream = make_projects_stream({"projects": "10 20"})
        stream.client.get.return_value = []  # no groups configured
        result = stream.get_project_ids()
        self.assertEqual(set(result), {"10", "20"})

    def test_group_projects_only(self):
        stream = make_projects_stream({"groups": "100"})
        stream.client.get.return_value = [{"id": 30}, {"id": 40}]
        result = stream.get_project_ids()
        self.assertEqual(set(result), {"30", "40"})

    def test_config_and_group_projects_merged(self):
        stream = make_projects_stream({"projects": "10 20", "groups": "100"})
        stream.client.get.return_value = [{"id": 30}]
        result = stream.get_project_ids()
        self.assertEqual(set(result), {"10", "20", "30"})

    def test_duplicate_ids_deduplicated(self):
        """A project in both config and group config should appear only once."""
        stream = make_projects_stream({"projects": "10 20", "groups": "100"})
        # Group 100 returns project 20 which is also in config
        stream.client.get.return_value = [{"id": 20}, {"id": 30}]
        result = stream.get_project_ids()
        self.assertEqual(set(result), {"10", "20", "30"})
        # No duplicates — length must equal unique count
        self.assertEqual(len(result), len(set(result)))

    def test_result_is_sorted(self):
        """Project IDs should be returned in stable sorted order."""
        stream = make_projects_stream({"projects": "30 10", "groups": "100"})
        stream.client.get.return_value = [{"id": 20}]
        result = stream.get_project_ids()
        self.assertEqual(result, sorted(result))

    def test_no_projects_no_groups_returns_empty(self):
        stream = make_projects_stream({})
        result = stream.get_project_ids()
        self.assertEqual(result, [])

    def test_empty_projects_string_and_no_groups_returns_empty(self):
        stream = make_projects_stream({"projects": "   "})
        result = stream.get_project_ids()
        self.assertEqual(result, [])

    def test_multiple_groups_all_fetched(self):
        stream = make_projects_stream({"groups": "100 200"})
        stream.client.get.side_effect = [
            [{"id": 10}],
            [{"id": 20}],
        ]
        result = stream.get_project_ids()
        self.assertEqual(set(result), {"10", "20"})
        self.assertEqual(stream.client.get.call_count, 2)


class TestGetRecords(unittest.TestCase):
    """Tests for Projects.get_records() — fetches each project by ID"""

    def test_fetches_each_project_id(self):
        stream = make_projects_stream({"projects": "10 20"})
        stream.client.get.side_effect = [
            {"id": 10, "name": "proj-a", "updated_at": "2026-01-01T00:00:00Z"},
            {"id": 20, "name": "proj-b", "updated_at": "2026-01-02T00:00:00Z"},
        ]
        records = list(stream.get_records())
        self.assertEqual(len(records), 2)
        self.assertEqual({r["id"] for r in records}, {10, 20})

    def test_skips_non_dict_responses(self):
        stream = make_projects_stream({"projects": "10 20"})
        stream.client.get.side_effect = [
            [{"unexpected": "list"}],  # non-dict — should be skipped
            {"id": 20, "updated_at": "2026-01-01T00:00:00Z"},
        ]
        records = list(stream.get_records())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 20)

    def test_uses_correct_endpoint_per_project(self):
        stream = make_projects_stream({"projects": "10"})
        stream.client.get.return_value = {"id": 10, "updated_at": "2026-01-01T00:00:00Z"}
        list(stream.get_records())
        called_endpoint = stream.client.get.call_args[0][0]
        self.assertEqual(called_endpoint, "https://gitlab.com/api/v4/projects/10")


class TestProjectsParentBookmark(unittest.TestCase):
    """Tests for ParentBaseStream bookmark behaviour on Projects."""

    def _make_projects_with_children(self, config=None):
        from tap_gitlab.streams.projects import Projects
        from tap_gitlab.streams.branches import Branches
        from tap_gitlab.streams.users import Users

        if config is None:
            config = {"start_date": "2026-01-01T00:00:00Z", "projects": "10"}
        client = make_mock_client(config)
        stream = Projects(client=client, catalog=make_mock_catalog_entry())
        branches = Branches(client=client, catalog=make_mock_catalog_entry())
        users = Users(client=client, catalog=make_mock_catalog_entry())
        stream.child_to_sync = [branches, users]
        return stream, branches, users

    def test_projects_extends_parent_base_stream(self):
        from tap_gitlab.streams.projects import Projects
        from tap_gitlab.streams.abstracts import ParentBaseStream
        self.assertTrue(issubclass(Projects, ParentBaseStream))

    def test_get_bookmark_returns_min_across_parent_and_children(self):
        """When children have an older bookmark, parent must re-process from children's date."""
        state = {
            "bookmarks": {
                "projects": {"updated_at": "2026-06-01T00:00:00Z"},
                "branches": {"projects_updated_at": "2026-03-01T00:00:00Z"},
                "users":    {"projects_updated_at": "2026-05-01T00:00:00Z"},
            }
        }
        stream, _, _ = self._make_projects_with_children()
        bookmark = stream.get_bookmark(state, "projects")
        self.assertEqual(bookmark, "2026-03-01T00:00:00Z")

    def test_get_bookmark_falls_back_to_start_date_when_no_state(self):
        stream, _, _ = self._make_projects_with_children()
        bookmark = stream.get_bookmark({}, "projects")
        self.assertEqual(bookmark, "2026-01-01T00:00:00Z")

    def test_update_bookmark_state_writes_children_under_projects_updated_at(self):
        """Children's bookmarks must be stored under 'projects_updated_at'."""
        state = {}
        stream, _, _ = self._make_projects_with_children()
        stream.update_bookmark_state(state, "projects", value="2026-07-01T00:00:00Z")
        self.assertEqual(
            state["bookmarks"]["branches"]["projects_updated_at"], "2026-07-01T00:00:00Z"
        )
        self.assertEqual(
            state["bookmarks"]["users"]["projects_updated_at"], "2026-07-01T00:00:00Z"
        )

    def test_update_bookmark_state_writes_parent_when_selected(self):
        from unittest.mock import patch
        state = {}
        stream, _, _ = self._make_projects_with_children()
        with patch.object(stream, "is_selected", return_value=True):
            stream.update_bookmark_state(state, "projects", value="2026-07-01T00:00:00Z")
        self.assertEqual(
            state["bookmarks"]["projects"]["updated_at"], "2026-07-01T00:00:00Z"
        )


class TestGroupsStreamIndependence(unittest.TestCase):

    def test_groups_sync_does_not_call_projects(self):
        from tap_gitlab.streams.groups import Groups
        client = make_mock_client({"groups": "100"})
        catalog_entry = make_mock_catalog_entry()
        stream = Groups(client=client, catalog=catalog_entry)

        # Groups should only have group_milestones as a child — no projects
        self.assertNotIn("projects", stream.children)

    def test_groups_has_no_custom_sync_method(self):
        from tap_gitlab.streams.groups import Groups
        from tap_gitlab.streams.abstracts import FullTableStream

        # Groups.sync should be inherited from FullTableStream, not overridden
        self.assertIs(Groups.sync, FullTableStream.sync)

    def test_groups_has_no_modify_object_collecting_project_ids(self):
        from tap_gitlab.streams.groups import Groups
        client = make_mock_client({"groups": "100"})
        catalog_entry = make_mock_catalog_entry()
        stream = Groups(client=client, catalog=catalog_entry)

        # Calling modify_object should not accumulate _collected_project_ids
        record = {"id": 100, "projects": [{"id": 1}, {"id": 2}]}
        stream.modify_object(record)
        self.assertFalse(hasattr(stream, "_collected_project_ids"))


class TestBranchesStream(unittest.TestCase):
    """Tests for Branches stream — now ChildBaseStream with updated_at from parent."""

    def _make_stream(self):
        from tap_gitlab.streams.branches import Branches
        from tap_gitlab.streams.abstracts import ChildBaseStream
        client = make_mock_client({})
        catalog_entry = make_mock_catalog_entry()
        return Branches(client=client, catalog=catalog_entry)

    def test_branches_is_child_base_stream(self):
        from tap_gitlab.streams.branches import Branches
        from tap_gitlab.streams.abstracts import ChildBaseStream
        self.assertTrue(issubclass(Branches, ChildBaseStream))

    def test_branches_replication_method_is_incremental(self):
        stream = self._make_stream()
        self.assertEqual(stream.replication_method, "INCREMENTAL")

    def test_branches_replication_key_is_projects_updated_at(self):
        stream = self._make_stream()
        self.assertEqual(stream.replication_keys, ["projects_updated_at"])

    def test_modify_object_sets_project_id_and_projects_updated_at_from_parent(self):
        stream = self._make_stream()
        parent = {"id": 42, "updated_at": "2026-03-01T10:00:00Z"}
        record = {"name": "main", "merged": False}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 42)
        self.assertEqual(result["projects_updated_at"], "2026-03-01T10:00:00Z")

    def test_modify_object_no_parent_returns_record_unchanged(self):
        stream = self._make_stream()
        record = {"name": "main"}
        result = stream.modify_object(record, None)
        self.assertNotIn("project_id", result)
        self.assertNotIn("updated_at", result)


class TestUsersStream(unittest.TestCase):
    """Tests for Users stream — now ChildBaseStream with updated_at from parent."""

    def _make_stream(self):
        from tap_gitlab.streams.users import Users
        client = make_mock_client({})
        catalog_entry = make_mock_catalog_entry()
        return Users(client=client, catalog=catalog_entry)

    def test_users_is_child_base_stream(self):
        from tap_gitlab.streams.users import Users
        from tap_gitlab.streams.abstracts import ChildBaseStream
        self.assertTrue(issubclass(Users, ChildBaseStream))

    def test_users_replication_method_is_incremental(self):
        stream = self._make_stream()
        self.assertEqual(stream.replication_method, "INCREMENTAL")

    def test_users_replication_key_is_projects_updated_at(self):
        stream = self._make_stream()
        self.assertEqual(stream.replication_keys, ["projects_updated_at"])

    def test_modify_object_sets_project_id_and_projects_updated_at_from_parent(self):
        stream = self._make_stream()
        parent = {"id": 99, "updated_at": "2026-06-15T08:00:00Z"}
        record = {"id": 1, "username": "alice"}
        result = stream.modify_object(record, parent)
        self.assertEqual(result["project_id"], 99)
        self.assertEqual(result["projects_updated_at"], "2026-06-15T08:00:00Z")

    def test_modify_object_no_parent_returns_record_unchanged(self):
        stream = self._make_stream()
        record = {"id": 1, "username": "alice"}
        result = stream.modify_object(record, None)
        self.assertNotIn("project_id", result)
        self.assertNotIn("updated_at", result)
