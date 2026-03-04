from base import BaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class AllFields(AllFieldsTest, BaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = {
        "groups": {
            "repository_storage",
            "ip_restriction_ranges"
        },
        "issues": {
            'assignee_id',
            'milestone_id',
            'subscribed',
            'author_id'
        },
        "projects": {
            'runners_token',
            'license_url',
            'mirror_user_id',
            'only_mirror_protected_branches',
            'license',
            'statistics',
            'mirror_overwrites_diverged_branches',
            'printing_merge_requests_link_enabled',
            'owner',
            'repository_storage',
            'mirror_trigger_builds',
            'ci_restrict_pipeline_cancellation_role',
            'approvals_before_merge',
            'mirror',
            'allow_pipeline_trigger_approve_deployment',
            'secret_push_protection_enabled',
        }
    }

    @staticmethod
    def name():
        return "tap_tester__all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)
