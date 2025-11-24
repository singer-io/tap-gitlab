import copy
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class BaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-gitlab"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.gitlab"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "projects": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "branches": {
                cls.PRIMARY_KEYS: { "project_id", "name" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "commits": {
                cls.PRIMARY_KEYS: { "id", "project_id" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "issues": {
                cls.PRIMARY_KEYS: { "id", "project_id" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "project_milestones": {
                cls.PRIMARY_KEYS: { "id", "project_id" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "group_milestones": {
                cls.PRIMARY_KEYS: { "id", "group_id" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "users": {
                cls.PRIMARY_KEYS: { "id", "project_id" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "groups": {
                cls.PRIMARY_KEYS: { "id" },
                cls.REPLICATION_METHOD: cls.full_table,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "private_token": "TAB_GITLAB_PRIVATE_TOKEN",
            "auth_header_key": "TAB_GITLAB_AUTH_HEADER_KEY",
            "auth_token_key": "TAP_GITLAB_AUTH_TOKEN_KEY",
            "groups": "TAP_GITLAB_GROUPS",
            "projects": "TAP_GITLAB_PROJECTS",
            "start_date": "TAP_GITLAB_START_DATE"
        }

        for key, env_var in creds.items():
            value = os.getenv(env_var)
            if value is None:
                LOGGER.warning(f"Environment variable {env_var} not set.")
            credentials_dict[key] = value
        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2022-07-01T00:00:00Z"
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value
