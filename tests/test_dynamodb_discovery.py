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
import boto3
from datetime import datetime, timedelta, timezone
from singer import utils, metadata
import singer

import decimal

from base import TestDynamoDBBase

LOGGER = singer.get_logger()
def create_table(client, table_name, hash_key, hash_type, sort_key, sort_type):
    print('\nCreating table: {}'.format(table_name))

    key_schema = [
        {
            'AttributeName': hash_key,
            'KeyType': 'HASH'  #Partition key
        },
    ]

    attribute_defs = [
        {
            'AttributeName': hash_key,
            'AttributeType': hash_type
        },
    ]

    if sort_key:
        key_schema.append({ 'AttributeName': sort_key, 'KeyType': 'RANGE' })
        attribute_defs.append({ 'AttributeName': sort_key, 'AttributeType': sort_type })
    
    client.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_defs,
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    print('Finished creating table: {}'.format(table_name))

def expected_table_config():
    return [
        {'TableName': 'simple_table_1',
         'HashKey': 'int_id',
         'HashType': 'N',
         'SortKey': 'string_field',
         'SortType': 'S',
         'generator': generate_simple_items_1},
        {'TableName': 'simple_table_2',
         'HashKey': 'string_id',
         'HashType': 'S',
         'SortKey': 'int_field',
         'generator': generate_simple_items_2,
         'SortType': 'N'},
        {'TableName': 'simple_table_3',
         'HashKey': 'int_id',
         'generator': generate_simple_items_1,
         'HashType': 'N'},
    ]

def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))
    

def generate_simple_items_1(num_items):
    for i in range(num_items):
        yield {'int_id': { 'N': str(i) },
               'string_field': {'S': random_string_generator() } }


def generate_simple_items_2(num_items):
    for i in range(num_items):
        yield {'string_id': { 'S': random_string_generator() },
               'int_field': {'N': str(i) } }

    
class DynamoDBDiscovery(TestDynamoDBBase):

    def setUp(self):
        client = boto3.client('dynamodb',
                              endpoint_url='http://localhost:8000',
                              region_name='us-east-1')

        table_configs = expected_table_config()

        self.clear_tables(client, (x['TableName'] for x in table_configs))

        for table in table_configs:
            create_table(client,
                         table['TableName'],
                         table['HashKey'],
                         table['HashType'],
                         table.get('SortKey'),
                         table.get('SortType'))

        waiter = client.get_waiter('table_exists')
        for table in table_configs:
            LOGGER.info('Adding Items for {}'.format(table['TableName']))
            waiter.wait(TableName=table['TableName'], WaiterConfig={"Delay": 1, "MaxAttempts": 20})        
            for item in table['generator'](50):
                client.put_item(TableName=table['TableName'], Item=item)



    def name(self):
        return "tap_tester_dynamodb_discovery"

    def tap_name(self):
        return "tap-dynamodb"

    def get_type(self):
        return "platform.dynamodb"

    def get_properties(self):
        return {
            "use_local_dynamo": 'true',
            "account_id": "123123123123",
            "region_name": "us-east-1"
        }

    def get_credentials(self):
        return {}

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # tap discovered the right streams
        catalog = menagerie.get_catalog(conn_id)

        table_configs = expected_table_config()

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


        client = boto3.client('dynamodb',
                              endpoint_url='http://localhost:8000',
                              region_name='us-east-1')

        self.clear_tables(client, (x['TableName'] for x in table_configs))

SCENARIOS.add(DynamoDBDiscovery)
