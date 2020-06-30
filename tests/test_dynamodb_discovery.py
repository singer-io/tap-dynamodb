from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
import string
import random
import time
import re
import pprint
import pdb
import paramiko
import csv
import json
from datetime import datetime, timedelta, timezone
from singer import utils, metadata
import singer
import decimal

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from base import TestDynamoDBBase

class DynamoDBDiscovery(TestDynamoDBBase):
    def expected_table_config(self):
        return [
            {'TableName': 'simple_table_1',
             'HashKey': 'int_id',
             'HashType': 'N',
             'SortKey': 'string_field',
             'SortType': 'S',
             'generator': self.generate_simple_items_1,
             'num_rows': 50,
            },
            {'TableName': 'simple_table_2',
             'HashKey': 'string_id',
             'HashType': 'S',
             'SortKey': 'int_field',
             'generator': self.generate_simple_items_2,
             'num_rows': 50,
             'SortType': 'N'},
            {'TableName': 'simple_table_3',
             'HashKey': 'int_id',
             'generator': self.generate_simple_items_1,
             'num_rows': 50,
             'HashType': 'N'},
        ]

    def generate_simple_items_1(self, num_items):
        serializer = TypeSerializer()
        for i in range(num_items):
            record = {
                'int_id': i,
                'string_field': self.random_string_generator(),
            }
            yield serializer.serialize(record)

    def generate_simple_items_2(self, num_items):
        serializer = TypeSerializer()
        for i in range(num_items):
            record = {
                'string_id': self.random_string_generator(),
                'int_field': i,
            }
            yield serializer.serialize(record)

    def name(self):
        return "tap_tester_dynamodb_discovery"

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # tap discovered the right streams
        catalog = menagerie.get_catalog(conn_id)

        table_configs = self.expected_table_config()

        for stream in catalog['streams']:
            # schema is open {} for each stream
            self.assertEqual({'type': 'object'}, stream['schema'])

        expected_streams = {x['TableName'] for x in table_configs}
        # assert we find the correct streams
        self.assertEqual(expected_streams,
                         {c['tap_stream_id'] for c in catalog['streams']})
        # Verify that the table_name is in the format <collection_name> for each stream
        self.assertEqual(expected_streams, {c['table_name'] for c in catalog['streams']})

        for tap_stream_id in expected_streams:
            found_stream = [c for c in catalog['streams'] if c['tap_stream_id'] == tap_stream_id][0]
            stream_metadata = [x['metadata'] for x in found_stream['metadata'] if x['breadcrumb']==[]][0]
            expected_config = [x for x in table_configs if x['TableName'] == tap_stream_id][0]

            # table-key-properties metadata
            keys = [expected_config['HashKey']]
            if expected_config.get('SortKey'):
                keys.append(expected_config.get('SortKey'))

            self.assertEqual(set(keys),
                             set(stream_metadata.get('table-key-properties')))

            # Assert the hash key is the first key in the list
            self.assertEqual(expected_config['HashKey'], stream_metadata.get('table-key-properties')[0])

            # row-count metadata
            self.assertEqual(50,
                             stream_metadata.get('row-count'))

            # selected metadata is None for all streams
            self.assertNotIn('selected', stream_metadata.keys())

            # is-view metadata is False
            self.assertFalse(stream_metadata.get('is-view'))

            # no forced-replication-method metadata
            self.assertNotIn('forced-replication-method', stream_metadata.keys())

SCENARIOS.add(DynamoDBDiscovery)
