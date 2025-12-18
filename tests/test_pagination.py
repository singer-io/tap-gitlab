from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import BaseTest

class PaginationTest(PaginationTest, BaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester__pagination_test"

    def streams_to_test(self):
        # don't have enough data to test pagination
        streams_to_exclude = {
            "group_milestones",
            "issues",
            "project_milestones"
        }
        return self.expected_stream_names().difference(streams_to_exclude)
