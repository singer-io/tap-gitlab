from typing import Dict, Any
from urllib.parse import quote
from singer import get_bookmark, get_logger
from tap_gitlab.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class GroupMilestones(IncrementalStream):
    tap_stream_id = "group_milestones"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    parent = "groups"
    replication_keys = ["updated_at"]
    data_key = None
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)
        return self.bookmark_value

    def get_url(self, parent_obj: Dict[str, Any]) -> str:
        group_identifier = parent_obj.get("full_path") or parent_obj.get("id")
        encoded_identifier = quote(group_identifier, safe="")
        return f"/groups/{encoded_identifier}/milestones"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        return f"{self.client.base_url}{self.get_url(parent_obj)}"
