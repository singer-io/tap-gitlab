from typing import Dict, Any
from urllib.parse import quote
from singer import get_logger
from tap_gitlab.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Branches(IncrementalStream):
    tap_stream_id = "branches"
    key_properties = ["project_id", "name"]
    replication_method = "INCREMENTAL"
    parent = "projects"
    replication_keys = ["committed_date"]
    data_key = None
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)
        return self.bookmark_value

    def get_url(self, parent_obj: Dict[str, Any]) -> str:
        if not parent_obj:
            raise ValueError("parent_obj is required but got None in Branches.get_url()")

        project_identifier = parent_obj.get("path_with_namespace") or parent_obj.get("project_id") or parent_obj.get("id")
        if not project_identifier:
            raise ValueError(f"Missing 'path_with_namespace' or 'project_id' in parent_obj: {parent_obj}")

        encoded_identifier = quote(str(project_identifier), safe="")
        return f"/projects/{encoded_identifier}/repository/branches"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        endpoint = f"{self.client.base_url}{self.get_url(parent_obj)}"
        LOGGER.info(f"[branches] Constructed endpoint: {endpoint}")
        return endpoint
