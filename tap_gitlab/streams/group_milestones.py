from tap_gitlab.streams.abstracts import ChildBaseStream


class GroupMilestones(ChildBaseStream):
    tap_stream_id = "group_milestones"
    key_properties = ["id", "group_id"]
    replication_method = "INCREMENTAL"
    parent = "groups"
    replication_keys = ["updated_at"]
    path = "groups/{}/milestones"
    data_key = None
    bookmark_value = None
