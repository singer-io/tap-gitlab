from typing import Dict, Iterator
from singer import get_logger, Transformer
from urllib.parse import quote

from tap_gitlab.streams.abstracts import FullTableStream


LOGGER = get_logger()

class Groups(FullTableStream):
    tap_stream_id = "groups"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = None
    path = "groups/{}"
    data_key = None
    children = ["group_milestones"]

    def get_group_ids(self) -> list:
        """Parse comma and/or space-separated group IDs from config."""
        groups_str = self.client.config.get("groups", "")
        if not groups_str:
            LOGGER.warning("No groups specified in config")
            return []

        group_ids = groups_str.replace(",", " ").split()
        LOGGER.info(f"Found {len(group_ids)} group IDs: {group_ids}")
        return group_ids

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Collect project IDs from group records."""
        # Check if this group has projects
        if 'projects' in record and isinstance(record['projects'], list):
            if not hasattr(self, '_collected_project_ids'):
                self._collected_project_ids = set()

            for project in record['projects']:
                if isinstance(project, dict) and 'id' in project:
                    encoded_id = quote(str(project['id']), safe='')
                    self._collected_project_ids.add(encoded_id)

        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Build endpoint URL for a specific group ID."""
        if hasattr(self, '_current_group_id'):
            encoded_id = quote(str(self._current_group_id), safe='')
            return f"{self.client.base_url}/groups/{encoded_id}"
        return f"{self.client.base_url}/groups"

    def get_records(self) -> Iterator:
        """Override to fetch records for each group ID from config."""
        group_ids = self.get_group_ids()

        for group_id in group_ids:
            try:
                self._current_group_id = group_id
                endpoint = self.get_url_endpoint()
                response = self.client.get(endpoint, self.params, self.headers, None)

                if isinstance(response, dict):
                    yield response
                else:
                    LOGGER.warning(f"Unexpected response type for group {group_id}: {type(response)}")

            except Exception as e:
                LOGGER.error(f"Error fetching group {group_id}: {str(e)}")
                continue

    def sync(
            self,
            state: Dict,
            transformer: Transformer,
            parent_obj: Dict = None,
            streams_to_sync: list = None,
            catalog=None
        ) -> int:
        """Override sync to collect project IDs and sync projects if selected."""
        # First, sync groups normally
        total_records = super().sync(state=state, transformer=transformer, parent_obj=parent_obj)

        # Import here to avoid circular dependency
        from tap_gitlab.streams.projects import Projects
        from tap_gitlab.sync import write_schema as sync_write_schema

        projects_stream = Projects(self.client, catalog.get_stream(Projects.tap_stream_id))

        if projects_stream.is_selected() and "groups" in streams_to_sync:
            # Get config project IDs
            config_projects = self.client.config.get("projects", "").strip().split()
            config_project_ids = set(config_projects) if config_projects and config_projects[0] else set()

            # Get group project IDs (if any were collected)
            group_project_ids = self._collected_project_ids if hasattr(self, '_collected_project_ids') else set()

            # Merge both sets to get all unique project IDs
            all_project_ids = config_project_ids | group_project_ids

            # Write schema for projects
            sync_write_schema(projects_stream, self.client, streams_to_sync, catalog)

            # Sync with all project IDs
            projects_stream.sync(
                state=state,
                transformer=transformer,
                project_ids_list=list(all_project_ids)
            )
        else:
            LOGGER.info("Projects stream not selected - skipping projects sync from group")

        if hasattr(self, '_collected_project_ids'):
            delattr(self, '_collected_project_ids')

        return total_records
