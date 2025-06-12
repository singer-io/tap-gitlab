from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_gitlab.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Projects(IncrementalStream):
    tap_stream_id = "projects"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]  # Use this instead of "last_activity_at"
    path = "/projects"
    data_key = None
