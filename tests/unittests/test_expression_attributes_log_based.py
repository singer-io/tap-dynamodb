import unittest
from unittest.mock import patch
from tap_dynamodb.sync_strategies.log_based import sync, sync_shard

CONFIG = {
    "start_date": "2017-01-01",
    "account_id": "dummy_account_id",
    "role_name": "dummy_role",
    "external_id": "dummy_external_id",
    "region_name": "dummy_region_name"
}
STATE = {}
STREAM = {
    "table_name": "GoogleDocs",
    "stream": "GoogleDocs",
    "tap_stream_id": "GoogleDocs",
    "metadata": []
}
class MockClient():
    def scan(self, **kwargs):
        return kwargs

    def describe_table(self, **kwargs):
        return {'Table': {'LatestStreamArn': 'dummy_arn'}}

    def describe_stream(self, **kwargs):
        return {'StreamDescription': {'Shards': [{'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}]}}

class MockDeserializer():
    def __init__(self):
        return {}

@patch('singer.metadata.to_map', return_value = {})
@patch('singer.write_state', return_value = {})
@patch('singer.write_bookmark', return_value = {})
@patch('singer.get_bookmark', return_value = {})
class TestExpressionAttributesInFullTable(unittest.TestCase):
    """Test expression attributes for reserved word in full table sync. Mocked some method of singer package"""

    @patch('singer.metadata.get', side_effect = ["#c, Sheet", "{\"#c\": \"Comment\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_single_expression(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for single reserve word passed in `expression` field."""
        client = MockClient()
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['Comment'], ['Sheet']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect = ["#tst[4], #n, Test", "{\"#tst\": \"test1\", \"#n\": \"Name\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_multiple_expression(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for multiple reserve words passed in `expression` field."""
        client = MockClient()
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1[4]'], ['Name'], ['Test']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["Comment, Sheet", ""])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_without_expression(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute with empty string passed in `expression` field."""
        client = MockClient()
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['Comment'], ['Sheet']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["", ""])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_without_projection(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute with empty string passed in `projection` field."""
        client = MockClient()
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#tst[4].#n, #tst[4].#a, Test", "{\"#tst\": \"test1\", \"#n\": \"Name\", \"#a\": \"Age\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_nested_expr_with_dict_and_list(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for nested reserved words with dictionary and list passed in `expression` field."""
        client = MockClient()
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1[4]', 'Name'], ['test1[4]', 'Age'], ['Test']], {}, 'GoogleDocs', {}, {})

    @patch('singer.metadata.get', side_effect =["#tst[4], Test", "{\"#tst\": \"test1\", \"#n\": \"Name\", \"#a\": \"Age\"}"])
    @patch('tap_dynamodb.sync_strategies.log_based.sync_shard', return_value = 1)
    @patch('tap_dynamodb.dynamodb.get_client')
    @patch('tap_dynamodb.dynamodb.get_stream_client')
    @patch('tap_dynamodb.deserialize.Deserializer', return_value = {})
    def test_sync_with_nested_expr_with_list(self, mock_deserializer, mock_stream_client, mock_client, mock_sync_shard, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for nested reserve words with list passed in `expression` field."""
        client = MockClient()
        mock_client.return_value = client
        mock_stream_client.return_value = client
        res = sync(CONFIG, STATE, STREAM)
        
        mock_sync_shard.assert_called_with({'SequenceNumberRange': {'EndingSequenceNumber': 'dummy_no'}, 'ShardId': 'dummy_id'}, {}, client, 'dummy_arn', [['test1[4]'], ['Test']], {}, 'GoogleDocs', {}, {})
