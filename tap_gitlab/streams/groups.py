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
                LOGGER.info(f"Syncing group: {group_id}")
                endpoint = self.get_url_endpoint()
                response = self.client.get(endpoint, self.params, self.headers, None)

                if isinstance(response, dict):
                    yield response
                else:
                    LOGGER.warning(f"Unexpected response type for group {group_id}: {type(response)}")

            except Exception as e:
                LOGGER.error(f"Error fetching group {group_id}: {str(e)}")
                continue
