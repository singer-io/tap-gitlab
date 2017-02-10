#!/usr/bin/env python3

import argparse
import datetime
import json
import os

import requests
import stitchstream
from strict_rfc3339 import rfc3339_to_timestamp


DEFAULT_URL = "https://gitlab.com/api/v3"
BASE_URL = None
PRIVATE_TOKEN = None
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_START_DATE = datetime.datetime(2016, 1, 1).strftime(DATETIME_FMT)
PER_PAGE = 100

state = {}

RESOURCES = {
    'projects': {
        'url': '/projects/{id}',
        'key_properties': ['id'],
    },
    'branches': {
        'url': '/projects/{id}/repository/branches',
        'key_properties': ['project_id', 'name'],
    },
    'commits': {
        'url': '/projects/{id}/repository/commits',
        'key_properties': ['id'],
    },
    'issues': {
        'url': '/projects/{id}/issues',
        'key_properties': ['id'],
    },
    'milestones': {
        'url': '/projects/{id}/milestones',
        'key_properties': ['id'],
    },
    'users': {
        'url': '/projects/{id}/users',
        'key_properties': ['id'],
    },
}

logger = stitchstream.get_logger()


class InvalidData(Exception):
    """Raise when data doesn't validate the schema"""


def _transform_datetime(value):
    return strp_gitlab(value).strftime(DATETIME_FMT)


def strp_gitlab(value):
    return datetime.datetime.utcfromtimestamp(rfc3339_to_timestamp(value))


def _anyOf(data, schema_list):
    for schema in schema_list:
        try:
            return transform_field(data, schema)
        except:
            pass

    raise InvalidData("{} doesn't match any of {}".format(data, schema_list))


def _array(data, items_schema):
    return [transform_field(value, items_schema) for value in data]


def _object(data, properties_schema):
    return {field: transform_field(data[field], field_schema)
            for field, field_schema in properties_schema.items()
            if field in data}


def _type_transform(value, type_schema):
    if isinstance(type_schema, list):
        for typ in type_schema:
            try:
                return _type_transform(value, typ)
            except:
                pass

        raise InvalidData("{} doesn't match any of {}".format(value, type_schema))

    if not value:
        if type_schema != "null":
            raise InvalidData("Null is not allowed")
        else:
            return None

    if type_schema == "string":
        return str(value)

    if type_schema == "integer":
        return int(value)

    if type_schema == "number":
        return float(value)

    if type_schema == "boolean":
        return bool(value)

    raise InvalidData("Unknown type {}".format(type_schema))


def _format_transform(value, format_schema):
    if format_schema == "date-time":
        return _transform_datetime(value)

    raise InvalidData("Unknown format {}".format(format_schema))


def transform_field(value, field_schema):
    if "anyOf" in field_schema:
        return _anyOf(value, field_schema["anyOf"])

    if field_schema["type"] == "array":
        return _array(value, field_schema["items"])

    if field_schema["type"] == "object":
        return _object(value, field_schema["properties"])

    value = _type_transform(value, field_schema["type"])
    if "format" in field_schema:
        value = _format_transform(value, field_schema["format"])

    return value


def transform_row(row, schema):
    return _object(row, schema["properties"])


def request(url, params=None):
    params = params or {}
    params['private_token'] = PRIVATE_TOKEN
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def _sync_subresource(resource, project, add_id=False, save_ids=False, transform=None):
    def _sync(d):
        for item in d:
            if add_id:
                item['project_id'] = project['id']

            if save_ids:
                project[resource].append(item['id'])

            if transform:
                item = transform(item)

            item = transform_row(item, RESOURCES[resource]['schema'])
            stitchstream.write_record(resource, item)

    if save_ids:
        project[resource] = []

    url = BASE_URL + RESOURCES[resource]['url'].format(id=project['id'])
    params = {'page': 1}
    resp = request(url, params=params)
    _sync(resp.json())

    last_page = int(resp.headers.get('X-Total-Pages', 1))
    for page in range(1, last_page + 1):
        params['page'] = page
        resp = request(url, params=params)
        _sync(resp.json())


def flatten_id(item, target):
    item[target + '_id'] = item.pop(target, {}).pop('id', None)
    return item


def sync_branches(project):
    def _transform(item):
        return flatten_id(item, "commit")

    _sync_subresource("branches", project, add_id=True, transform=_transform)


def sync_commits(project):
    _sync_subresource("commits", project, add_id=True)


def sync_issues(project):
    def _transform(item):
        item = flatten_id(item, "milestone")
        item = flatten_id(item, "author")
        item = flatten_id(item, "assignee")
        return item

    _sync_subresource("issues", project, transform=_transform)


def sync_milestones(project):
    _sync_subresource("milestones", project)


def sync_users(project):
    _sync_subresource("users", project, save_ids=True)


def sync_project(project_id):
    if isinstance(project_id, str):
        pid = project_id.replace("/", "%2F")
    else:
        pid = project_id

    url = BASE_URL + RESOURCES["projects"]["url"].format(id=pid)
    resp = request(url)
    data = resp.json()

    data['owner_id'] = data.pop('owner', {}).pop('id', None)
    project = transform_row(data, RESOURCES["projects"]["schema"])

    start_time = datetime.datetime.strptime(state.get(project_id, DEFAULT_START_DATE), DATETIME_FMT)
    last_activity_at = datetime.datetime.strptime(project['last_activity_at'], DATETIME_FMT)
    if last_activity_at >= start_time:
        sync_branches(project)
        sync_commits(project)
        sync_issues(project)
        sync_milestones(project)
        sync_users(project)

        stitchstream.write_record("projects", project)
        state[project_id] = project['last_activity_at']
        stitchstream.write_state(state)


def do_sync(project_ids):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "tap_gitlab")
    for resource, config in RESOURCES.items():
        with open(path + "/" + resource + ".json") as f:
            schema = json.load(f)

        config['schema'] = schema
        stitchstream.write_schema(resource, schema, config['key_properties'])

    for project_id in project_ids:
        sync_project(project_id)


def main():
    global BASE_URL
    global PRIVATE_TOKEN

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    BASE_URL = config.get('url', DEFAULT_URL)
    PRIVATE_TOKEN = config['private_token']

    if args.state:
        logger.info("Loading state from " + args.state)
        with open(args.state) as f:
            state.update(json.load(f))

    do_sync(config['projects'])


if __name__ == '__main__':
    main()
