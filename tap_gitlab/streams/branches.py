from typing import Dict, Any
from urllib.parse import quote
from tap_gitlab.streams.abstracts import FullTableStream


class Branches(FullTableStream):
    tap_stream_id = "branches"
    key_properties = ["project_id", "name"]
    replication_method = "FULL_TABLE"
    parent = "projects"
    replication_keys = None
    path = "projects/{}/repository/branches"
    data_key = None

    def get_url(self, parent_obj: Dict[str, Any]) -> str:
        """Construct the URL for fetching users of a specific project."""
        if not parent_obj:
            raise ValueError("parent_obj is required but got None in users.get_url()")

        project_identifier = parent_obj.get("id")
        if not project_identifier:
            raise ValueError(f"Missing project identifier in parent_obj: {parent_obj}")

        encoded_project_id = quote(str(project_identifier), safe="")
        return self.path.format(encoded_project_id)

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Construct the full URL endpoint for the users stream."""
        endpoint = f"{self.client.base_url}/{self.get_url(parent_obj)}"
        return endpoint

    def modify_object(self, record, parent_record=None):
        """Adding project_id and last_committed_date to the record"""
        if isinstance(record, dict):
            if parent_record and isinstance(parent_record, dict):
                record["project_id"] = parent_record.get("id")

        return record
