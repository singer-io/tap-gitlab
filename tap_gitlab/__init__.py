#!/usr/bin/env python3

import datetime
import sys
import os
import pytz

import backoff
import requests
import singer

from singer import Transformer, utils
from strict_rfc3339 import rfc3339_to_timestamp

PER_PAGE = 100
CONFIG = {
    'api_url': "https://gitlab.com/api/v3",
    'private_token': None,
    'start_date': None,
}
STATE = {}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

RESOURCES = {
    'projects': {
        'url': '/projects/{}',
        'schema': load_schema('projects'),
        'key_properties': ['id'],
    },
    'branches': {
        'url': '/projects/{}/repository/branches',
        'schema': load_schema('branches'),
        'key_properties': ['project_id', 'name'],
    },
    'commits': {
        'url': '/projects/{}/repository/commits',
        'schema': load_schema('commits'),
        'key_properties': ['id'],
    },
    'issues': {
        'url': '/projects/{}/issues',
        'schema': load_schema('issues'),
        'key_properties': ['id'],
    },
    'milestones': {
        'url': '/projects/{}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
    },
    'users': {
        'url': '/projects/{}/users',
        'schema': load_schema('users'),
        'key_properties': ['id'],
    },
}


LOGGER = singer.get_logger()
SESSION = requests.Session()


def get_url(entity, pid):
    if not isinstance(pid, int):
        pid = pid.replace("/", "%2F")

    return CONFIG['api_url'] + RESOURCES[entity]['url'].format(pid)


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, # pylint: disable=line-too-long
                      factor=2)
def request(url, params=None):
    params = params or {}
    params['private_token'] = CONFIG['private_token']

    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    if resp.status_code >= 400:
        LOGGER.critical(
            "Error making request to GitLab API: GET {} [{} - {}]".format(
                req.url, resp.status_code, resp.content))
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

def transform_row(data, typ, schema):
    result = data
    if schema.get('format') == 'date-time':
        dt = datetime.datetime.utcfromtimestamp(rfc3339_to_timestamp(data)).replace(tzinfo=pytz.UTC)
        result = utils.strftime(dt)

    return result

def flatten_id(item, target):
    if target in item and item[target] is not None:
        item[target + '_id'] = item.pop(target, {}).pop('id', None)
    else:
        item[target + '_id'] = None


def sync_branches(project):
    url = get_url("branches", project['id'])
    with Transformer(pre_hook=transform_row) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            flatten_id(row, "commit")
            transformed_row = transformer.transform(row, RESOURCES["branches"]["schema"])
            singer.write_record("branches", transformed_row, time_extracted=utils.now())


def sync_commits(project):
    url = get_url("commits", project['id'])
    with Transformer(pre_hook=transform_row) as transformer:
        for row in gen_request(url):
            row['project_id'] = project["id"]
            try:
                transformed_row = transformer.transform(row, RESOURCES["commits"]["schema"])
            except:
                import pdb
                pdb.set_trace()
            singer.write_record("commits", transformed_row, time_extracted=utils.now())


def sync_issues(project):
    url = get_url("issues", project['id'])
    with Transformer(pre_hook=transform_row) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "assignee")
            flatten_id(row, "milestone")
            transformed_row = transformer.transform(row, RESOURCES["issues"]["schema"])

            if row["updated_at"] >= get_start("project_{}".format(project["id"])):
                singer.write_record("issues", transformed_row, time_extracted=utils.now())


def sync_milestones(project):
    url = get_url("milestones", project['id'])
    with Transformer(pre_hook=transform_row) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["milestones"]["schema"])

            if row["updated_at"] >= get_start("project_{}".format(project["id"])):
                singer.write_record("milestones", transformed_row, time_extracted=utils.now())


def sync_users(project):
    url = get_url("users", project['id'])
    project["users"] = []
    with Transformed(pre_hook=transform_row) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["users"]["schema"])
            project["users"].append(row["id"])
            singer.write_record("users", transformed_row, time_extracted=utils.now())


def sync_project(pid):
    url = get_url("projects", pid)
    data = request(url).json()
    time_extracted = utils.now()

    with Transformer(pre_hook=transform_row) as transformer:
        flatten_id(data, "owner")
        project = transformer.transform(data, RESOURCES["projects"]["schema"])

        state_key = "project_{}".format(project["id"])

        #pylint: disable=maybe-no-member
        last_activity_at = project.get('last_activity_at', project.get('created_at'))
        if not last_activity_at:
            raise Exception(
                #pylint: disable=line-too-long
                "There is no last_activity_at or created_at field on project {}. This usually means I don't have access to the project."
                .format(project['id']))


        if project['last_activity_at'] >= get_start(state_key):

            sync_branches(project)
            sync_commits(project)
            sync_issues(project)
            sync_milestones(project)
            sync_users(project)

            singer.write_record("projects", project, time_extracted=time_extracted)
            utils.update_state(STATE, state_key, last_activity_at)
            singer.write_state(STATE)


def do_sync(pids):
    LOGGER.info("Starting sync")

    for resource, config in RESOURCES.items():
        singer.write_schema(resource, config['schema'], config['key_properties'])

    for pid in pids:
        sync_project(pid)

    LOGGER.info("Sync complete")


def main_impl():
    args = utils.parse_args(["private_token", "projects", "start_date"])

    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    do_sync(CONFIG['projects'].split(' '))


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
