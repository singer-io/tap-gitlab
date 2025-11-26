#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='tap-gitlab',
    version='0.6.0',
    description='Singer.io tap for extracting data from the GitLab API',
    author='Stitch',
    url='https://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    install_requires=[
        'singer-python==6.1.1',
        'requests==2.32.5',
        'backoff==2.2.1'
    ],
    entry_points='''
        [console_scripts]
        tap-gitlab=tap_gitlab:main
    ''',
    packages=find_packages(),
    package_data={
        'tap_gitlab': ['schemas/*.json', 'schemas/shared/*.json']
    },
    include_package_data=True,
)
