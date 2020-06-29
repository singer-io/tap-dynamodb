from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import csv
import boto3
import singer

from base import TestDynamoDBBase

LOGGER = singer.get_logger()


class DynamoDBFullTableInterruptible(TestDynamoDBBase):
    def expected_table_config(self):
        return [
            {'TableName': 'simple_table_1',
            'HashKey': 'int_id',
            'HashType': 'N',
            'SortKey': 'string_field',
            'SortType': 'S',
            'generator': self.generate_items,
            'num_rows': 3531},
        ]

    def setUp(self):
        client = boto3.client('dynamodb',
                              endpoint_url='http://localhost:8000',
                              region_name='us-east-1')

        table_configs = self.expected_table_config()

        self.clear_tables(client)

        for table in table_configs:
            self.create_table(client,
                              table['TableName'],
                              table['HashKey'],
                              table['HashType'],
                              table.get('SortKey'),
                              table.get('SortType'))

        waiter = client.get_waiter('table_exists')
        for table in table_configs:
            LOGGER.info('Adding Items for {}'.format(table['TableName']))
            waiter.wait(TableName=table['TableName'], WaiterConfig={"Delay": 1, "MaxAttempts": 20})
            for item in table['generator'](table['num_rows']):
                client.put_item(TableName=table['TableName'], Item=item['M'])

    def name(self):
        return "tap_tester_dynamodb_full_table_interruptible"

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


        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        found_catalogs = menagerie.get_catalogs(conn_id)
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   stream_catalog,
                                                                                   annotated_schema,
                                                                                   additional_md)

        # This was experimentally found to be the interrupted state after
        # syncing 1000 records
        interrupted_state = {
            'currently_syncing': 'simple_table_1',
            'bookmarks': {
                'simple_table_1': {
                    'version': '1574362046060',
                    'last_evaluated_key': {
                        'int_id': {'N': '107'},
                        'string_field': {'S': '1078'}
                    }
                }
            }
        }

        menagerie.set_state(conn_id, interrupted_state)
            
        # run full table sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()

        expected_pks = {}

        for config in table_configs:
            key = { config['HashKey'] }
            if config.get('SortKey'):
                key |= { config.get('SortKey') }
            expected_pks[config['TableName']] = key

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   { x['TableName'] for x in table_configs },
                                                                   expected_pks)

        state = menagerie.get_state(conn_id)

        first_versions = {}

        # assert that we get the correct number of records for each stream
        for config in table_configs:
            table_name = config['TableName']

            self.assertEqual(config['num_rows']-1000,
                             record_count_by_stream[table_name])

            # activateVersionMessage as the last message and not the first
            self.assertNotEqual('activate_version',
                                records_by_stream[table_name]['messages'][0]['action'])
            self.assertEqual('activate_version',
                             records_by_stream[table_name]['messages'][-1]['action'])

            # assert that the state has an initial_full_table_complete == True
            self.assertTrue(state['bookmarks'][table_name]['initial_full_table_complete'])

            # assert that there is a version bookmark in state and it is
            # the same version as the state passed in
            self.assertEqual(interrupted_state['bookmarks'][table_name]['version'],
                             state['bookmarks'][table_name]['version'])

            self.assertIsNone(state['bookmarks'][table_name].get('last_evaluated_key'))

            
            self.assertTrue(state['bookmarks'][table_name].get('initial_full_table_complete', False))

        client = boto3.client('dynamodb',
                              endpoint_url='http://localhost:8000',
                              region_name='us-east-1')


        self.clear_tables(client)

SCENARIOS.add(DynamoDBFullTableInterruptible)
