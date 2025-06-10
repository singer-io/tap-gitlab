#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-gitlab',
    version='0.5.1',
    description='Singer.io tap for extracting data from the GitLab API',
    author='Stitch',
    url='https://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    install_requires=[
        'singer-python==5.0.4',
        'requests==2.20.0',
        'strict-rfc3339==0.7',
        'backoff==1.3.2',
        'pytz'
    ],
    entry_points='''
        [console_scripts]
        tap-gitlab=tap_gitlab:main
    ''',
    packages=['tap_gitlab'],
    package_data={
        'tap_gitlab': ['schemas/*.json', 'schemas/shared/*.json']
    },
    include_package_data=True,
)
