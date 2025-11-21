from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_gitlab.streams.abstracts import IncrementalStream
from tap_gitlab.streams.group_milestones import GroupMilestones  # import child stream

LOGGER = get_logger()

class Groups(IncrementalStream):
    tap_stream_id = "groups"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["created_at"]  # Use a datetime-based field
    path = "groups"
    data_key = None
    bookmark_value = None
    children = ["group_milestones"]

    def __init__(self, client, catalog):
        super().__init__(client, catalog)

        # Register child stream so it pulls after each group
        self.child_to_sync = [
            GroupMilestones(client, catalog)
        ]

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value.
        """
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
