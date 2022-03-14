import decimal

import singer

from boto3.dynamodb.types import TypeSerializer

from tap_tester import connections
from tap_tester import menagerie
from tap_tester import runner

from base import TestDynamoDBBase

LOGGER = singer.get_logger()


class DynamoDBFullTable(TestDynamoDBBase):
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
                'int_id': i,
                'decimal_field': decimal.Decimal(str(i) + '.00000000001'),
                'string_field': self.random_string_generator(),
                'byte_field': b'some_bytes',
                'int_list_field': [i, i+1, i+2],
                'int_set_field': set([i, i+1, i+2]),
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
        return "tap_tester_dynamodb_full_table"

    def test_run(self):
        (table_configs, conn_id, _) = self.pre_sync_test()

        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        found_catalogs = menagerie.get_catalogs(conn_id)
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{"breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]
            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               stream_catalog,
                                                               annotated_schema,
                                                               additional_md)

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

        first_versions = {}

        # assert that we get the correct number of records for each stream
        for config in table_configs:
            table_name = config['TableName']

            self.assertEqual(config['num_rows'],
                             record_count_by_stream[table_name])

            # assert that an activate_version_message is first and last message sent for each stream
            self.assertEqual('activate_version',
                             records_by_stream[table_name]['messages'][0]['action'])
            self.assertEqual('activate_version',
                             records_by_stream[table_name]['messages'][-1]['action'])

            # assert that the state has an initial_full_table_complete == True
            self.assertTrue(state['bookmarks'][table_name]['initial_full_table_complete'])
            # assert that there is a version bookmark in state
            first_versions[table_name] = state['bookmarks'][table_name]['version']
            self.assertIsNotNone(first_versions[table_name])
