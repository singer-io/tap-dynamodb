import decimal
import singer

from boto3.dynamodb.types import TypeSerializer

from tap_tester.scenario import (SCENARIOS)
from tap_tester import connections
from tap_tester import menagerie
from tap_tester import runner

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

    def generate_items(self, num_items):
        serializer = TypeSerializer()
        for i in range(num_items):
            record = {
                'int_id': int(i / 10.0),
                'decimal_field': decimal.Decimal(str(i) + '.00000000001'),
                'string_field': str(i),
                'byte_field': b'some_bytes',
                'int_list_field': [i, i + 1, i + 2],
                'int_set_field': set([i, i + 1, i + 2]),
                'map_field': {
                    'map_entry_1': 'map_value_1',
                    'map_entry_2': 'map_value_2'
                },
                'string_list': [self.random_string_generator(), self.random_string_generator(), self.random_string_generator()],
                'boolean_field': True,
                'other_boolean_field': False,
                'null_field': None
            }
            yield serializer.serialize(record)

    @staticmethod
    def name():
        return "tap_tester_dynamodb_full_table_interruptible"

    def test_run(self):
        (table_configs, conn_id, _) = self.pre_sync_test()

        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        found_catalogs = menagerie.get_catalogs(conn_id)
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'FULL_TABLE'}}]
            connections.select_catalog_and_fields_via_metadata(conn_id,
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
            key = {config['HashKey']}
            if config.get('SortKey'):
                key |= {config.get('SortKey')}
            expected_pks[config['TableName']] = key

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   {x['TableName'] for x in table_configs},
                                                                   expected_pks)

        state = menagerie.get_state(conn_id)

        # assert that we get the correct number of records for each stream
        for config in table_configs:
            table_name = config['TableName']

            self.assertEqual(config['num_rows'] - 1000,
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


SCENARIOS.add(DynamoDBFullTableInterruptible)
