from typing import Dict, Any
from urllib.parse import quote
from singer import get_bookmark, get_logger
from tap_gitlab.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Users(IncrementalStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "INCREMENTAL" # need to verify if this is correct
    parent = "projects"
    replication_keys = None
    data_key = None
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)
        return self.bookmark_value

    def get_url(self, parent_obj: Dict[str, Any]) -> str:
        if not parent_obj:
            LOGGER.warning("Users stream called independently without parent_obj. Skipping.")
            return None

        project_id = parent_obj.get("id")
        if not project_id:
            LOGGER.error("Missing project_id in parent_obj for Users stream.")
            return None

        encoded_project_id = quote(str(project_id), safe="")
        return f"projects/{encoded_project_id}/users"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        url = self.get_url(parent_obj)
        if not url:
            return ""
        return f"{self.client.base_url}{url}"
