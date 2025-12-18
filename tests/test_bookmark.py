from base import BaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class BookMarkTest(BookmarkTest, BaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%SZ"
    initial_bookmarks = {
        "bookmarks": {
        }
    }
    @staticmethod
    def name():
        return "tap_tester__bookmark_test"

    def streams_to_test(self):
        # Exclude streams that are full table or don't have enough data
        streams_to_exclude = {
            'users',
            'branches',
            'groups',
            'group_milestones',
            'issues',
            'project_milestones'
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def get_bookmark_value(self, state, stream):
        """
        Get the bookmark value for a given stream from state.
        For commits stream, use 'committed_date', for others use 'updated_at'.
        """
        stream_id = self.get_stream_id(stream)
        stream_bookmark = state.get('bookmarks', {}).get(stream_id, {})

        if stream == 'commits':
            bookmark_value = stream_bookmark.get('committed_date')
        else:
            bookmark_value = stream_bookmark.get('updated_at')

        if bookmark_value and '.000000Z' in bookmark_value:
            bookmark_value = bookmark_value.replace('.000000Z', 'Z')

        return bookmark_value

    def calculate_new_bookmarks(self):
        """
        Override to handle streams with insufficient data.
        Calculates new bookmarks by looking through sync 1 data to determine a bookmark
        that will sync 2 records in sync 2 (plus any necessary look back data).
        """
        new_bookmarks = {}
        replication_methods = self.expected_replication_methods
        replication_keys = self.expected_replication_keys()
        for stream, records in BookmarkTest.synced_records_1.items():
            replication_method = replication_methods.get(stream, {})
            if replication_method == self.INCREMENTAL:
                look_back = self.expected_lookback_window(stream)
                replication_key = replication_keys[stream]
                assert len(replication_key) == 1
                replication_key = next(iter(replication_key))

                # get the replication values that are prior to the lookback window
                replication_values = sorted({
                    message['data'][replication_key] for message in records['messages']
                    if message['action'] == 'upsert'
                       and self.parse_date(message['data'][replication_key]) <
                       self.parse_date(self.get_bookmark_value(
                           BookmarkTest.state_1, self.get_stream_id(stream))) - look_back})
                print(f"unique replication values for stream {stream} are: {replication_values}")

                # Skip streams with insufficient data
                if len(replication_values) < 2:
                    print(f"WARNING: Stream {stream} does not have enough replication values "
                          f"(found {len(replication_values)}, need at least 2). Skipping bookmark calculation.")
                    continue

                # There should be 3 or more records (prior to the look back window)
                # so we can set the bookmark to get the last 2 records (+ the look back)
                new_bookmarks[self.get_stream_id(stream)] = {
                    replication_key:
                        self.timedelta_formatted(self.parse_date(replication_values[-2]),
                                                 date_format=self.bookmark_format)}
        return new_bookmarks

    def test_first_sync_bookmark(self):
        """Override to compare without microseconds since GitLab bookmarks don't save them."""
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_methods.get(stream,{})

                sync_1_records = [
                    record['data'] for record in
                    self.synced_records_1.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                if replication_method == self.INCREMENTAL:
                    # gather expectations
                    expected_replication_key = self.expected_replication_keys(stream)
                    # Make sure this is not a compound replication key
                    assert len(expected_replication_key) == 1
                    expected_replication_key = next(iter(expected_replication_key))

                    # Verify the first sync bookmark value is the max replication key value
                    max_replication_value = max(
                        self.parse_date(record.get(expected_replication_key))
                        for record in sync_1_records)
                    bookmark_value = self.parse_date(self.bookmark_values_1.get(stream,{}))

                    # GitLab doesn't save microseconds in bookmarks, so compare without them
                    max_replication_no_micro = max_replication_value.replace(microsecond=0)
                    bookmark_no_micro = bookmark_value.replace(microsecond=0)
                    self.assertEqual(max_replication_no_micro, bookmark_no_micro)

    def test_second_sync_bookmark(self):
        """Override to compare without microseconds since GitLab bookmarks don't save them."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_methods.get(stream,{})

                # gather results
                sync_2_records = [
                    record['data'] for record in
                    self.synced_records_2.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                if replication_method == self.INCREMENTAL:
                    expected_replication_key = self.expected_replication_keys(stream)
                    # Make sure this is not a compound replication key
                    assert len(expected_replication_key) == 1
                    expected_replication_key = next(iter(expected_replication_key))

                    # Verify the second sync bookmark value is the max replication key value
                    max_replication_value = max(
                        self.parse_date(record.get(expected_replication_key))
                        for record in sync_2_records)
                    bookmark_value = self.parse_date(self.bookmark_values_2.get(stream,{}))

                    # GitLab doesn't save microseconds in bookmarks, so compare without them
                    max_replication_no_micro = max_replication_value.replace(microsecond=0)
                    bookmark_no_micro = bookmark_value.replace(microsecond=0)
                    self.assertEqual(max_replication_no_micro, bookmark_no_micro)
