from typing import Dict, Any
from urllib.parse import quote
from singer import get_bookmark, get_logger
from tap_gitlab.streams.abstracts import FullTableStream

LOGGER = get_logger()

class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties = ["id", "project_id"]
    replication_method = "FULL_TABLE"
    parent = "projects"
    replication_keys = None
    data_key = None

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
        return f"{self.client.base_url}/{url}"

    def modify_object(self, record, parent_record = None):
        """Adding project_id to the record."""
        if isinstance(record, dict):
            record["project_id"] = parent_record.get("id")

        return record
