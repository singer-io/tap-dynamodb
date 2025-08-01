#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-dynamodb",
    version="1.4.1",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_dynamodb"],
    install_requires=[
        'boto3==1.39.8',
        'urllib3==2.5.0',
        "singer-python==6.0.1",
        'backoff==2.2.1',
    ],
    extras_require={
        'dev': [
            'ipdb',
            'pylint==3.0.4',
            'nose2'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-dynamodb=tap_dynamodb:main
    """,
    packages=["tap_dynamodb", 'tap_dynamodb.sync_strategies'],
    package_data = {},
    include_package_data=True,
)
