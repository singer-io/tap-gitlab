from typing import Dict, Any, Iterator
from singer import get_logger
from urllib.parse import quote

from tap_gitlab.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Projects(IncrementalStream):
    tap_stream_id = "projects"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = "projects/{}"
    data_key = None
    children = ["branches", "issues", "commits", "project_milestones", "users"]

    def get_group_project_ids(self) -> set:
        """Fetch project IDs from all groups configured in config, regardless of whether groups stream is selected."""
        groups_str = self.client.config.get("groups", "")
        if not groups_str or not groups_str.strip():
            return set()

        group_ids = groups_str.strip().replace(",", " ").split()
        LOGGER.info(f"Fetching projects from {len(group_ids)} configured group(s)")

        project_ids = set()
        for group_id in group_ids:
            encoded_id = quote(str(group_id), safe='')
            endpoint = f"{self.client.base_url}/groups/{encoded_id}/projects"
            page = 1
            while True:
                params = {"per_page": self.page_size, "page": page}
                response = self.client.get(endpoint, params, self.headers, None)
                if not isinstance(response, list) or not response:
                    break
                for project in response:
                    if isinstance(project, dict) and 'id' in project:
                        project_ids.add(str(project['id']))
                if len(response) < self.page_size:
                    break
                page += 1

        LOGGER.info(f"Found {len(project_ids)} project ID(s) from configured groups")
        return project_ids

    def get_project_ids(self) -> list:
        """Get all project IDs: from config + from all configured groups."""
        config_str = self.client.config.get("projects", "")
        config_ids = set(config_str.strip().replace(",", " ").split()) if config_str.strip() else set()

        group_ids = self.get_group_project_ids()

        all_ids = config_ids | group_ids
        if not all_ids:
            LOGGER.warning("No project IDs found from config or groups")
            return []

        LOGGER.info(
            f"Total project IDs to sync: {len(all_ids)} "
            f"(config: {len(config_ids)}, groups: {len(group_ids)})"
        )
        return sorted(all_ids)

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Build endpoint URL for a specific project ID."""
        if hasattr(self, '_current_project_id'):
            encoded_id = quote(str(self._current_project_id), safe='')
            return f"{self.client.base_url}/projects/{encoded_id}"
        return f"{self.client.base_url}/projects"

    def sync(self, state: Dict, transformer: Any, parent_obj: Dict = None) -> int:
        """Sync all projects from config IDs and configured groups."""
        LOGGER.info("Syncing projects from config and configured groups")
        return super().sync(state=state, transformer=transformer, parent_obj=None)

    def get_records(self) -> Iterator:
        """Fetch records for each resolved project ID."""
        project_ids = self.get_project_ids()

        for project_id in project_ids:
            self._current_project_id = project_id
            LOGGER.info(f"Syncing project: {project_id}")
            endpoint = self.get_url_endpoint()
            response = self.client.get(endpoint, self.params, self.headers, None)

            if isinstance(response, dict):
                yield response
            else:
                LOGGER.warning(f"Unexpected response type for project {project_id}: {type(response)}")
