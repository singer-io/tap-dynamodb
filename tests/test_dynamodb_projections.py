import decimal
import singer

from boto3.dynamodb.types import TypeSerializer

from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from base import TestDynamoDBBase

LOGGER = singer.get_logger()


class DynamoDBProjections(TestDynamoDBBase):
    def expected_table_config(self):
        return [
            {'TableName': 'simple_table_1',
             'HashKey': 'int_id',
             'HashType': 'N',
             'SortKey': 'string_field',
             'SortType': 'S',
             'generator': self.generate_items,
             'num_rows': 100,
             'ProjectionExpression': 'int_id, string_field, decimal_field, int_list_field[1], map_field.map_entry_1, string_list[2], map_field.list_entry[2], list_map[1].a',
             'top_level_keys': {'int_id', 'string_field', 'decimal_field', 'int_list_field', 'map_field', 'string_list', 'list_map'},
             'top_level_list_keys': {'int_list_field', 'string_list', 'list_map'},
             'nested_map_keys': {'map_field': {'map_entry_1', 'list_entry'}},
             'map_projection': {'map_field': {'map_entry_1': 'map_value_1'}}
            },
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
                    'map_entry_2': 'map_value_2',
                    'list_entry': [i, i+1, i+2]
                },
                'list_map': [
                    {'a': 1,
                     'b': 2},
                    {'a': 100,
                     'b': 200}
                ],
                'string_list': [self.random_string_generator(), self.random_string_generator(), self.random_string_generator()],
                'boolean_field': True,
                'other_boolean_field': False,
                'null_field': None
            }
            yield serializer.serialize(record)

    @staticmethod
    def name():
        return "tap_tester_dynamodb_projections"

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


        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        found_catalogs = menagerie.get_catalogs(conn_id)
        for stream_catalog in found_catalogs:
            expected_config = [x for x in table_configs if x['TableName'] == stream_catalog['tap_stream_id']][0]
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{"breadcrumb" : [], "metadata" : {
                'replication-method' : 'FULL_TABLE',
                'tap-mongodb.projection': expected_config['ProjectionExpression']
            }}]
            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               stream_catalog,
                                                               annotated_schema,
                                                               additional_md)

        # run full table sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()

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
                             messages_by_stream[table_name]['messages'][0]['action'])
            self.assertEqual('activate_version',
                             messages_by_stream[table_name]['messages'][-1]['action'])

            # assert that the state has an initial_full_table_complete == True
            self.assertTrue(state['bookmarks'][table_name]['initial_full_table_complete'])
            # assert that there is a version bookmark in state
            first_versions[table_name] = state['bookmarks'][table_name]['version']
            self.assertIsNotNone(first_versions[table_name])

            # assert that the projection causes the correct fields to be returned
            for message in messages_by_stream[table_name]['messages']:
                if message['action'] == 'upsert':
                    if not message['data'].get('_sdc_deleted_at'):
                        top_level_keys = {*message['data'].keys()}
                        self.assertEqual(config['top_level_keys'], top_level_keys)
                        for list_key in config['top_level_list_keys']:
                            self.assertTrue(isinstance(message['data'][list_key], list))
                        self.assertEqual(config['nested_map_keys']['map_field'], {*message['data']['map_field'].keys()})

SCENARIOS.add(DynamoDBProjections)
