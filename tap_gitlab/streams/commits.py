from tap_gitlab.streams.abstracts import ChildBaseStream


class Commits(ChildBaseStream):
    tap_stream_id = "commits"
    key_properties = ["id", "project_id"]
    replication_method = "INCREMENTAL"
    parent = "projects"
    replication_keys = ["committed_date"]
    path = "projects/{}/repository/commits"
    data_key = None
    bookmark_value = None

    def modify_object(self, record, parent_record = None):
        """Adding project_id to the record."""
        if isinstance(record, dict):
            record["project_id"] = parent_record.get("id")

        return record
