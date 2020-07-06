import singer

from boto3.dynamodb.types import TypeSerializer

from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestDynamoDBBase

LOGGER = singer.get_logger()


class DynamoDBLogBased(TestDynamoDBBase):
    def expected_table_config(self):
        return [
            {'TableName': 'com-stitchdata-test-dynamodb-integration-simple_table_1',
             'HashKey': 'int_id',
             'HashType': 'N',
             'generator': self.generate_items,
             'num_rows': 100},
        ]

    def generate_items(self, num_items, start_key=0):
        serializer = TypeSerializer()
        for i in range(start_key, start_key + num_items):
            record = {
                'int_id': i,
                'string_field': self.random_string_generator(),
                'boolean_field': True,
            }
            yield serializer.serialize(record)

    @staticmethod
    def name():
        return "tap_tester_dynamodb_log_based"

    def test_run(self):
        (table_configs, conn_id, expected_streams) = self.pre_sync_test()

        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        found_catalogs = menagerie.get_catalogs(conn_id)
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{"breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               stream_catalog,
                                                               annotated_schema,
                                                               additional_md)

        # Disable streams forces shards to close
        self.disableStreams(expected_streams)
        self.first_sync_test(table_configs, conn_id)

        ################################
        # Run sync again and check that shard is read again, same number of records as last time, but this time it should get it from the shard(s)
        ################################
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
        state_version = menagerie.get_state_version(conn_id)

        first_versions = {}

        # assert that we get the correct number of records for each stream
        for config in table_configs:
            table_name = config['TableName']

            self.assertEqual(config['num_rows'],
                             record_count_by_stream[table_name])

            # assert that an activate_version_message is first only
            self.assertEqual('activate_version',
                             records_by_stream[table_name]['messages'][0]['action'])

            # assert that the state has an initial_full_table_complete == True
            self.assertTrue(state['bookmarks'][table_name]['initial_full_table_complete'])
            # assert that there is a version bookmark in state
            first_versions[table_name] = state['bookmarks'][table_name]['version']
            self.assertIsNotNone(first_versions[table_name])
            # assert shard_seq_numbers recorded for this table, implying a shard's data was read
            self.assertIn('shard_seq_numbers', state['bookmarks'][table_name])

            # Write state one 10 sequence numbers prior to current one
            # For one shard and remove that shard from finished_shards
            # Assumes the first shard removed has at least that many records.
            # The first sync should only have 1 shard, but theoretically this is not
            # gauranteed
            # This should result in the next sync having 10 messages
            shard_id_to_remove = state['bookmarks'][table_name]['finished_shards'].pop()
            shard_from_dynamodb = self.get_shard(table_name, shard_id_to_remove)
            shard_to_removed_last_sequence_number = shard_from_dynamodb['SequenceNumberRange']['EndingSequenceNumber']
            new_shard_last_sequence_number = int(shard_to_removed_last_sequence_number) - 10
            # Minimum length of shard sequence number is 21 so pad with leading zeros
            # See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html#DDB-Type-streams_StreamRecord-SequenceNumber
            new_shard_last_sequence_number_string = str(new_shard_last_sequence_number).zfill(21)
            state['bookmarks'][table_name]['shard_seq_numbers'][shard_id_to_remove] = new_shard_last_sequence_number_string
            menagerie.set_state(conn_id, state, version=state_version)

        ################################
        # Run sync again and check that expected records came through
        ################################
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()

        # Check that we only have 11 message (10 sequence numbers + activate_version) on syncing
        # a stream without changes
        for stream in records_by_stream.values():
            self.assertEqual(11, len(stream['messages']))

        ################################
        # Run sync again and check that no records came through
        ################################
        # Disable streams forces shards to close
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()

        # Check that we only have 1 message (activate_version) on syncing
        # a stream without changes
        for stream in records_by_stream.values():
            self.assertEqual(1, len(stream['messages']))

    def first_sync_test(self, table_configs, conn_id):
        # run first full table sync
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
        state_version = menagerie.get_state_version(conn_id)

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

            # Write state with missing finished_shards so it
            # re-reads data from all shards
            # This should result in the next sync having same number of records
            # as the full table sync
            state['bookmarks'][table_name].pop('finished_shards')
            menagerie.set_state(conn_id, state, version=state_version)



SCENARIOS.add(DynamoDBLogBased)
