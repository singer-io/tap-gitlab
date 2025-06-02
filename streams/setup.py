from setuptools import setup, find_packages

setup(
    name='tap-gitlab',
    version='1.0.0',
    description='Singer.io tap for extracting data from the GitLab API',
    author='Your Name',
    url='http://singer.io',
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
    ],
    packages=find_packages(),  # Will automatically include tap_sample
    install_requires=[
        'singer-python==6.1.1',
        'requests==2.32.3',
        'strict-rfc3339==0.7',
        'backoff==2.2.1'
    ],
    entry_points='''
        [console_scripts]
        tap-gitlab=tap_sample.__init__:main
    ''',
    package_data={
        'tap_sample': ['schemas/*.json'],
    },
    include_package_data=True,
)
