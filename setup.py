#!/usr/bin/env python

from setuptools import setup

setup(name='tap-gitlab',
      version='0.5.1',
      description='Singer.io tap for extracting data from the GitLab API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gitlab'],
      install_requires=[
          'singer-python==5.13.2',
          'requests==2.32.4',
          'strict-rfc3339==0.7',
          'backoff==1.10.0'
      ],
      entry_points='''
          [console_scripts]
          tap-gitlab=tap_gitlab:main
      ''',
      packages=['tap_gitlab'],
      package_data = {
          'tap_gitlab/schemas': [
            "branches.json",
            "commits.json",
            "issues.json",
            "milestones.json",
            "projects.json",
            "users.json",
          ],
      },
      include_package_data=True,
)
