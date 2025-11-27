from tap_gitlab.streams.abstracts import ChildBaseStream


class Branches(ChildBaseStream):
    tap_stream_id = "branches"
    key_properties = ["project_id", "name"]
    replication_method = "INCREMENTAL"
    parent = "projects"
    replication_keys = ["last_committed_date"]
    path = "projects/{}/repository/branches"
    data_key = None
    bookmark_value = None

    def modify_object(self, record, parent_record=None):
        """Adding project_id and last_committed_date to the record"""
        if isinstance(record, dict):
            if parent_record and isinstance(parent_record, dict):
                record["project_id"] = parent_record.get("id")

            commit = record.get("commit")
            if commit and isinstance(commit, dict):
                committed_date = commit.get("committed_date")
                if committed_date:
                    record["last_committed_date"] = committed_date
                else:
                    record["last_committed_date"] = self.client.config.get("start_date")
            else:
                record["last_committed_date"] = self.client.config.get("start_date")

        return record
