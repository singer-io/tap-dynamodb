import random
import unittest
import string
import decimal

import boto3
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

import singer

from tap_tester import connections
from tap_tester import menagerie
from tap_tester import runner

ALL_TABLE_NAMES_TO_CLEAR = frozenset({
    'simple_table_1',
    'simple_table_2',
    'simple_table_3',
    'simple_table_4',
    'com-stitchdata-test-dynamodb-integration-simple_table_1',
})

LOGGER = singer.get_logger()


class TestDynamoDBBase(unittest.TestCase):
    _client = None
    _streams_client = None
    decimal_index = 0

    def expected_table_config(self):
        raise NotImplementedError

    def setUp(self):
        client = self.dynamodb_client()

        table_configs = self.expected_table_config()

        self.clear_tables()

        for table in table_configs:
            self.create_table(client,
                              table['TableName'],
                              table['HashKey'],
                              table['HashType'],
                              table.get('SortKey'),
                              table.get('SortType'))

        waiter = client.get_waiter('table_exists')
        for table in table_configs:
            LOGGER.info('Adding Items for %s', table['TableName'])
            waiter.wait(TableName=table['TableName'], WaiterConfig={"Delay": 1, "MaxAttempts": 20})
            for item in table['generator'](table['num_rows']):
                client.put_item(TableName=table['TableName'], Item=item['M'])

    def tearDown(self):
        self.clear_tables()

    @staticmethod
    def tap_name():
        return "tap-dz-dynamodb"

    @staticmethod
    def get_type():
        return "platform.dynamodb"

    @staticmethod
    def get_properties():
        return {
            "use_local_dynamo": 'true',
            "account_id": "123123123123",
            "region_name": "us-east-1"
        }

    @staticmethod
    def get_credentials():
        return {}

    def dynamodb_client(self):
        if not self._client:
            self._client = boto3.client('dynamodb',
                                        endpoint_url='http://localhost:8000',
                                        region_name='us-east-1')
        return self._client

    def dynamodb_streams_client(self):
        if not self._streams_client:
            self._streams_client = boto3.client('dynamodbstreams',
                                                endpoint_url='http://localhost:8000',
                                                region_name='us-east-1')

        return self._streams_client

    def create_table(self, client, table_name, hash_key, hash_type, sort_key, sort_type):
        self.assertIn(table_name, ALL_TABLE_NAMES_TO_CLEAR)
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
            key_schema.append({'AttributeName': sort_key, 'KeyType': 'RANGE'})
            attribute_defs.append({'AttributeName': sort_key, 'AttributeType': sort_type})

        client.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_defs,
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_IMAGE'
            },
        )
        print('Finished creating table: {}'.format(table_name))

    def clear_tables(self, table_names=ALL_TABLE_NAMES_TO_CLEAR):
        client = self.dynamodb_client()
        existing_table_names = client.list_tables()['TableNames']
        existing_table_names_to_clear = table_names.intersection(set(existing_table_names))
        for table_name in existing_table_names_to_clear:
            try:
                client.delete_table(TableName=table_name)
            except Exception as e:
                print('\nCould not clear table {} due to error {}'.format(table_name, e))

        # wait for all tables to be deleted
        waiter = client.get_waiter('table_not_exists')
        for table_name in existing_table_names_to_clear:
            try:
                waiter.wait(TableName=table_name, WaiterConfig={"Delay": 3, "MaxAttempts": 20})
            except Exception as e:
                print('\nCould not wait on table {} due to error {}'.format(table_name, e))

    @staticmethod
    def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))


    def decimal_generator(self):
        """
        Use a global decimal_index to cycle through the decimal_list below containing a wide array
        of interesting decimal values and boundary cases. After reaching the bottom of the list
        the cycle continues from the top of the list.
        """
        decimal_list = [
            '12345', '-21', '98765432109876543210987654321098765432',
            '0', '-3', '8', '-0',
            '0.99999999999999999999999999999999999999e126',
            '-0.99999999999999999999999999999999999999e126',
            '9.9999999999999999999999999999999999999e-128',
            '-9.9999999999999999999999999999999999999e-128',
            '00000000000000000000.000000000000000000e100',
            '1.598738596902e55',
            '-6897395.09111e8',
            '0.000000000000083729e-3',
            '-73840.84957394e76',
            '9.99999999999999999999999999777e123',
            '-9.88888888888888888888888888888e-124',
            '0.1234567890e88',
            '-0.98765432109876543210e-99' ]

        num = decimal_list[self.decimal_index % len(decimal_list)]
        self.decimal_index += 1

        return decimal.Decimal(num)

    def enableStreams(self, table_names):
        client = self.dynamodb_client()

        for table_name in table_names:
            client.update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_IMAGE'
                },
            )

    def disableStreams(self, table_names):
        client = self.dynamodb_client()

        for table_name in table_names:
            client.update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': False,
                },
            )

    def addMoreData(self, numRows):
        client = self.dynamodb_client()

        table_configs = self.expected_table_config()

        for table in table_configs:
            LOGGER.info('Adding %s Items for %s', numRows, table['TableName'])
            for item in table['generator'](numRows, table['num_rows']):
                client.put_item(TableName=table['TableName'], Item=item['M'])

    def updateData(self, numRows, start_key, field_key, field_value):
        client = self.dynamodb_client()
        deserializer = TypeDeserializer()
        serializer = TypeSerializer()

        table_configs = self.expected_table_config()

        for table in table_configs:
            LOGGER.info('Updating %s Items by setting field with key %s to the value %s, with start_key %s, for table %s', numRows, field_key, field_value, start_key, table['TableName'])
            for item in table['generator'](numRows, start_key):
                record = deserializer.deserialize(item)
                hashKey = table['HashKey']
                key = {
                    hashKey: serializer.serialize(record[hashKey])
                }
                serializedFieldValue = serializer.serialize(field_value)
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
                client.update_item(
                    TableName=table['TableName'],
                    Key=key,
                    UpdateExpression='set {}=:v'.format(field_key),
                    ExpressionAttributeValues={
                        ':v': serializedFieldValue,
                    },
                )

    def deleteData(self, id_range):
        client = self.dynamodb_client()

        for table in self.expected_table_config():
            LOGGER.info('Deleting %s Items for %s', len(id_range), table['TableName'])
            for i in id_range:
                client.delete_item(TableName=table['TableName'],
                                   Key={'int_id': {
                                       'N': str(i)}})

    def get_shard(self, table_name, shard_id):
        table = self.dynamodb_client().describe_table(TableName=table_name)['Table']
        stream_arn = table['LatestStreamArn']

        params = {
            'StreamArn': stream_arn
        }

        has_more = True

        while has_more:
            stream_info = self.dynamodb_streams_client().describe_stream(**params)['StreamDescription']

            for shard in stream_info['Shards']:
                if shard['ShardId'] == shard_id:
                    return shard

            last_evaluated_shard_id = stream_info.get('LastEvaluatedShardId')
            has_more = last_evaluated_shard_id is not None

            if has_more:
                params['ExclusiveStartShardId'] = last_evaluated_shard_id

        raise ValueError('shardId {} not found in stream {} for table {}'.format(shard_id, stream_arn, table_name))

    def pre_sync_test(self):
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
            stream_metadata = [x['metadata'] for x in found_stream['metadata'] if x['breadcrumb'] == []][0]
            expected_config = [x for x in table_configs if x['TableName'] == tap_stream_id][0]

            # table-key-properties metadata
            keys = [expected_config['HashKey']]
            if expected_config.get('SortKey'):
                keys.append(expected_config.get('SortKey'))

            self.assertEqual(set(keys),
                             set(stream_metadata.get('table-key-properties')))

            # Assert the hash key is the first key in the list
            self.assertEqual(expected_config['HashKey'],
                             stream_metadata.get('table-key-properties')[0])

            # row-count metadata
            self.assertEqual(expected_config['num_rows'],
                             stream_metadata.get('row-count'))

            # selected metadata is None for all streams
            self.assertNotIn('selected', stream_metadata.keys())

            # is-view metadata is False
            self.assertFalse(stream_metadata.get('is-view'))

            # no forced-replication-method metadata
            self.assertNotIn('forced-replication-method', stream_metadata.keys())

        return (table_configs, conn_id, expected_streams)
