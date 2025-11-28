from tap_gitlab.streams.abstracts import ChildBaseStream


class ProjectMilestones(ChildBaseStream):
    tap_stream_id = "project_milestones"
    key_properties = ["id", "project_id"]
    replication_method = "INCREMENTAL"
    parent = "projects"
    replication_keys = ["updated_at"]
    path = "projects/{}/milestones"
    data_key = None
    bookmark_value = None

    def modify_object(self, record, parent_record = None):
        """Adding project_id to the record."""
        if isinstance(record, dict) and "project_id" not in record:
            record["project_id"] = parent_record.get("id")

        return record
