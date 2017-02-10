#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path


setup(name='tap-gitlab',
      version='0.1.1',
      description='Taps GitLab data',
      author='Stitch',
      url='https://github.com/stitchstreams/tap-gitlab',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gitlab'],
      install_requires=[
          'stitchstream-python>=0.5.0',
          'requests==2.12.4',
          'strict-rfc3339==0.7',
      ],
      entry_points='''
          [console_scripts]
          tap-gitlab=tap_gitlab:main
      ''',
      packages=['tap_gitlab'],
      package_data = {
          'tap_gitlab': [
            "branches.json",
            "commits.json",
            "issues.json",
            "milestones.json",
            "projects.json",
            "users.json",
          ],
      }
)
