#!/usr/bin/env python3

import datetime
import sys

import requests
import singer

from . import utils
from .transform import transform_row


PER_PAGE = 100
CONFIG = {
    'api_url': "https://gitlab.com/api/v3",
    'private_token': None,
    'start_date': None,
}
STATE = {}

RESOURCES = {
    'projects': {
        'url': '/projects/{}',
        'schema': utils.load_schema('projects'),
        'key_properties': ['id'],
    },
    'branches': {
        'url': '/projects/{}/repository/branches',
        'schema': utils.load_schema('branches'),
        'key_properties': ['project_id', 'name'],
    },
    'commits': {
        'url': '/projects/{}/repository/commits',
        'schema': utils.load_schema('commits'),
        'key_properties': ['id'],
    },
    'issues': {
        'url': '/projects/{}/issues',
        'schema': utils.load_schema('issues'),
        'key_properties': ['id'],
    },
    'milestones': {
        'url': '/projects/{}/milestones',
        'schema': utils.load_schema('milestones'),
        'key_properties': ['id'],
    },
    'users': {
        'url': '/projects/{}/users',
        'schema': utils.load_schema('users'),
        'key_properties': ['id'],
    },
}

logger = singer.get_logger()
session = requests.Session()


def get_url(entity, pid):
    if not isinstance(pid, int):
        pid = pid.replace("/", "%2F")

    return CONFIG['api_url'] + RESOURCES[entity]['url'].format(pid)


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def request(url, params=None):
    params = params or {}
    params['private_token'] = CONFIG['private_token']

    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    logger.info("GET {}".format(req.url))
    resp = session.send(req)

    if resp.status_code >= 400:
        logger.error("GET {} [{} - {}]".format(req.url, resp.status_code, resp.content))
        sys.exit(1)

    return resp


def gen_request(url):
    params = {'page': 1}
    resp = request(url, params)
    last_page = int(resp.headers.get('X-Total-Pages', 1))

    for row in resp.json():
        yield row

    for page in range(2, last_page + 1):
        params['page'] = page
        resp = request(url, params)
        for row in resp.json():
            yield row


def flatten_id(item, target):
    if target in item and item[target] is not None:
        item[target + '_id'] = item.pop(target, {}).pop('id', None)
    else:
        item[target + '_id'] = None


def sync_branches(project):
    url = get_url("branches", project['id'])
    for row in gen_request(url):
        row['project_id'] = project['id']
        flatten_id(row, "commit")
        row = transform_row(row, RESOURCES["branches"]["schema"])
        singer.write_record("branches", row)


def sync_commits(project):
    url = get_url("commits", project['id'])
    for row in gen_request(url):
        row['project_id'] = project["id"]
        row = transform_row(row, RESOURCES["commits"]["schema"])
        singer.write_record("commits", row)


def sync_issues(project):
    url = get_url("issues", project['id'])
    for row in gen_request(url):
        flatten_id(row, "author")
        flatten_id(row, "assignee")
        flatten_id(row, "milestone")
        row = transform_row(row, RESOURCES["issues"]["schema"])
        if row["updated_at"] >= get_start("project_{}".format(project["id"])):
            singer.write_record("issues", row)


def sync_milestones(project):
    url = get_url("milestones", project['id'])
    for row in gen_request(url):
        row = transform_row(row, RESOURCES["milestones"]["schema"])
        if row["updated_at"] >= get_start("project_{}".format(project["id"])):
            singer.write_record("milestones", row)


def sync_users(project):
    url = get_url("users", project['id'])
    project["users"] = []
    for row in gen_request(url):
        row = transform_row(row, RESOURCES["users"]["schema"])
        project["users"].append(row["id"])
        singer.write_record("users", row)


def sync_project(pid):
    url = get_url("projects", pid)
    data = request(url).json()

    flatten_id(data, "owner")
    project = transform_row(data, RESOURCES["projects"]["schema"])

    state_key = "project_{}".format(project["id"])
    last_activity_at = project.get('last_activity_at', project.get('created_at'))
    if last_activity_at >= get_start(state_key):
        sync_branches(project)
        sync_commits(project)
        sync_issues(project)
        sync_milestones(project)
        sync_users(project)

        singer.write_record("projects", project)
        utils.update_state(STATE, state_key, last_activity_at)
        singer.write_state(STATE)


def do_sync(pids):
    logger.info("Starting sync")

    for resource, config in RESOURCES.items():
        singer.write_schema(resource, config['schema'], config['key_properties'])

    for pid in pids:
        sync_project(pid)

    logger.info("Sync complete")


def main():
    args = utils.parse_args()

    config = utils.load_json(args.config)
    utils.check_config(config, ["private_token", "projects", "start_date"])
    CONFIG.update(config)

    if args.state:
        STATE.update(utils.load_json(args.state))

    do_sync(CONFIG['projects'].split(' '))


if __name__ == '__main__':
    main()
