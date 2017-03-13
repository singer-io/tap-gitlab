import argparse
import datetime
import functools
import json
import os
import threading

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

#pylint: disable=invalid-name

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    return parser.parse_args()


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))
