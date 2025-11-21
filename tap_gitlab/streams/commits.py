from typing import Dict, Any
from urllib.parse import quote
from singer import get_logger
from tap_gitlab.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Commits(IncrementalStream):
    tap_stream_id = "commits"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    parent = "projects"
    replication_keys = ["created_at"]
    data_key = None

    def get_url(self, parent_obj: Dict[str, Any]) -> str:
        if not parent_obj:
            raise ValueError("parent_obj is required but got None in Commits.get_url()")
        project_identifier = parent_obj.get("path_with_namespace") or parent_obj.get("id")
        encoded_identifier = quote(str(project_identifier), safe="")
        return f"projects/{encoded_identifier}/repository/commits"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        return f"{self.client.base_url}{self.get_url(parent_obj)}"
