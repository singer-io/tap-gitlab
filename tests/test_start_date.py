from base import BaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class StartDateTest(StartDateTest, BaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester__start_date_test"

    def streams_to_test(self):
        # don't have enough data to test start_date
        streams_to_exclude = {
            "users",
            "branches",
            "groups"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2015-03-25T00:00:00Z"

    @property
    def start_date_2(self):
        return "2017-01-25T00:00:00Z"

    def test_replicated_records(self):
        """
        Override because GitLab streams don't obey start_date.
        All streams should replicate the same records regardless of start_date.
        """
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                expected_primary_keys = self.expected_primary_keys(stream)

                assert len(self.expected_replication_keys().get(stream)) == 1
                expected_replication_key = next(iter(self.expected_replication_keys().get(stream)))

                replication_dates_1 = {
                    record['data'].get(expected_replication_key) for record in
                    self.synced_messages_by_stream_1.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}

                primary_keys_sync_1 = {
                    tuple(message['data'][expected_pk] for expected_pk in expected_primary_keys)
                    for message in self.synced_messages_by_stream_1.get(stream, {}).get('messages', [])
                    if message.get('action') == 'upsert'}

                primary_keys_sync_2 = {
                    tuple(message['data'][expected_pk] for expected_pk in expected_primary_keys)
                    for message in self.synced_messages_by_stream_2.get(stream, {}).get('messages', [])
                    if message.get('action') == 'upsert'
                    and self.parse_date(message['data'][expected_replication_key])
                    <= self.parse_date(max(replication_dates_1))}

                self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2,
                                   msg=f"Stream {stream} does not obey start date, "
                                       f"both syncs should have the same records")
