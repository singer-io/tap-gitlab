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

    def get_project_ids(self) -> list:
        """Parse space-separated project IDs from config."""
        projects_str = self.client.config.get("projects", "")
        if not projects_str:
            LOGGER.warning("No projects specified in config")
            return []

        project_ids = projects_str.strip().split()
        LOGGER.info(f"Found {len(project_ids)} project IDs: {project_ids}")
        return project_ids

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Build endpoint URL for a specific project ID."""
        if hasattr(self, '_current_project_id'):
            encoded_id = quote(str(self._current_project_id), safe='')
            return f"{self.client.base_url}/projects/{encoded_id}"
        return f"{self.client.base_url}/projects"

    def sync(self, state: Dict, transformer: Any, parent_obj: Dict = None, project_ids_list: list = None) -> int:
        """Override sync to handle both config project IDs and group projects."""
        if project_ids_list:
            # Called with explicit project IDs list (e.g., from groups)
            self._project_ids_override = project_ids_list
            result = super().sync(state=state, transformer=transformer, parent_obj=None)
            delattr(self, '_project_ids_override')
            return result

        # Called independently - sync projects from config
        LOGGER.info("Syncing projects from config project IDs")
        return super().sync(state=state, transformer=transformer, parent_obj=None)

    def get_records(self) -> Iterator:
        """Override to fetch records for each project ID from config."""
        # Use override list if provided, otherwise get from config
        if hasattr(self, '_project_ids_override'):
            project_ids = self._project_ids_override
            LOGGER.info(f"Using overridden project IDs list with {project_ids} projects")
        else:
            project_ids = self.get_project_ids()

        for project_id in project_ids:
            self._current_project_id = project_id
            endpoint = self.get_url_endpoint()
            response = self.client.get(endpoint, self.params, self.headers, None)

            if isinstance(response, dict):
                yield response
            else:
                LOGGER.warning(f"Unexpected response type for project {project_id}: {type(response)}")
