from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_sample.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Branches(IncrementalStream):
    tap_stream_id = "branches"
    key_properties = ["project_id", "name"]
    replication_method = "INCREMENTAL"
    path = "/projects/{project_id}/repository/branches"
    parent = "projects"
    replication_keys = ["commit_id"]  # Based on available fields
    data_key = None
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None):
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
