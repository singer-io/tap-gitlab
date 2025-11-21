from typing import Dict, Any
from urllib.parse import quote
from singer import get_logger
from tap_gitlab.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class ProjectMilestones(IncrementalStream):
    tap_stream_id = "project_milestones"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    parent = "projects"
    replication_keys = ["updated_at"]
    data_key = None
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)
        return self.bookmark_value

    def get_url(self, parent_obj: Dict[str, Any]) -> str:
        if not parent_obj:
            LOGGER.warning("ProjectMilestones stream called independently without parent_obj. Skipping.")
            return None
        project_identifier = parent_obj.get("path_with_namespace") or parent_obj.get("id")
        if not project_identifier:
            LOGGER.error("Missing project identifier in parent_obj.")
            return None
        encoded_identifier = quote(str(project_identifier), safe="")
        return f"projects/{encoded_identifier}/milestones"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        url = self.get_url(parent_obj)
        if not url:
            return ""
        return f"{self.client.base_url}{url}"
