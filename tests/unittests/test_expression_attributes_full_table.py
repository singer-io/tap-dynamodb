import unittest
from unittest.mock import patch
from tap_dynamodb.sync_strategies.full_table import sync, scan_table

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

@patch('singer.metadata.to_map', return_value = {})
@patch('singer.write_state', return_value = {})
@patch('singer.write_bookmark', return_value = {})
@patch('singer.get_bookmark', return_value = {})
class TestExpressionAttributesInFullTable(unittest.TestCase):
    """Test expression attributes for reserved word in full table sync. Mocked some method of singer package"""

    @patch('singer.metadata.get', side_effect = ["Comment, Sheet", "Comment"])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_sync_with_single_expression(self, mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for single reserve word passed in `expression` field."""
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', '#Comment,Sheet', {'#Comment': 'Comment'}, {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})


    @patch('tap_dynamodb.dynamodb.get_client', return_value = MockClient())        
    def test_scan_table_with_single_expression(self, mock_get_client, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that scan method called with extra `ExpressionAttributeNames` parameter for single reserve word. 
        Here we mocked get_client with return value as object of MockClient class. Mocked scan method to return arguments 
        which we passed."""
        res = list(scan_table('', '#Comment,Sheet', {'#Comment': 'Comment'}, {}, {}))

         # Verify that `scan` method called with expected paramater including `ExpressionAttributeNames` and `ProjectionExpression`
        self.assertEqual(res, [{'ExclusiveStartKey': {},'ExpressionAttributeNames': {'#Comment': 'Comment'},'Limit': 1000,
                                'ProjectionExpression': '#Comment,Sheet','TableName': ''}])

    @patch('singer.metadata.get', side_effect = ["Comment, Sheet", "Comment, Sheet"])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_sync_with_multiple_expression(self, mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for multiple reserve word passed in `expression` field."""
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including multiple expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', '#Comment,#Sheet', {'#Comment': 'Comment', '#Sheet': 'Sheet'}, {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})


    @patch('tap_dynamodb.dynamodb.get_client', return_value = MockClient())        
    def test_scan_table_with_multiple_expression(self, mock_get_client, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that scan method called with extra `ExpressionAttributeNames` parameter for multiple reserve word. 
        Here we mocked get_client with return value as object of MockClient class. Mocked scan method to return arguments 
        which we passed."""
        res = list(scan_table('', '#Comment,#Sheet', {'#Comment': 'Comment', '#Sheet': 'Sheet'}, {}, {}))
        
         # Verify that `scan` method called with expected paramater including `ExpressionAttributeNames` and `ProjectionExpression`
        self.assertEqual(res, [{'ExclusiveStartKey': {},'ExpressionAttributeNames': {'#Comment': 'Comment', '#Sheet': 'Sheet'},
                                'Limit': 1000, 'ProjectionExpression': '#Comment,#Sheet','TableName': ''}])
        
    @patch('singer.metadata.get', side_effect = ["Comment, Sheet", ""])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_sync_without_expression(self, mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for no reserve word passed in `expression` field. """
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including empty expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', 'Comment, Sheet', '', {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})

    @patch('tap_dynamodb.dynamodb.get_client', return_value = MockClient())        
    def test_scan_table_without_expression(self, mock_get_client, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that scan method does not called with extra `ExpressionAttributeNames` parameter when no reserve word passed 
        Here we mocked get_client with return value as object of MockClient class. Mocked scan method to return arguments
        which we passed."""
        res = list(scan_table('', 'Comment,Sheet', {}, {}, {}))

        # Verify that `scan` method called with expected paramater excluding `ExpressionAttributeNames`
        self.assertEqual(res, [{'ExclusiveStartKey': {},'Limit': 1000, 'ProjectionExpression': 'Comment,Sheet','TableName': ''}])
  
    @patch('singer.metadata.get', side_effect = ["", ""])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_sync_without_projection(self, mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for empty value passed in `projection` field. """
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including empty expression attributes and projection
        mock_scan_table.assert_called_with('GoogleDocs', '', '', {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})

    @patch('tap_dynamodb.dynamodb.get_client', return_value = MockClient())        
    def test_scan_table_without_projection(self, mock_get_client, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test that scan method does not called with `ExpressionAttributeNames` and `ProjectionExpression` parameter when empty value passed 
        in `projection` Here we mocked get_client with return value as object of MockClient class. Mocked scan method to return arguments 
        which we passed."""
        res = list(scan_table('', '', '', {}, {}))
        
        # Verify that `scan` method called with expected paramater excluding `ExpressionAttributeNames` and `ProjectionExpression`
        self.assertEqual(res, [{'ExclusiveStartKey': {},'Limit': 1000,'TableName': ''}])

    @patch('singer.metadata.get', side_effect = ["test.tests[1], test.tests[2]", "test.tests[1], test.tests[2]"])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_scan_table_with_nested_expr_with_dict_and_list(self,mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for multiple reserve word passed in `expression` field."""
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including nested expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', '#test.#tests1[1],#test.#tests2[2]', {'#test': 'test', '#tests1': 'tests', '#tests2': 'tests'}, {}, {'start_date': '2017-01-01', 
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})

    @patch('singer.metadata.get', side_effect = ["tests[1], tests[2]", "tests[1], tests[2]"])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_scan_table_with_nested_expr_with_list(self,mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for multiple reserve word passed in `expression` field."""
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including nested expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', '#tests1[1],#tests2[2]', {'#tests1': 'tests', '#tests2': 'tests'}, {}, {'start_date': '2017-01-01',
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})

    @patch('singer.metadata.get', side_effect = ["tests.x", "tests.x"])
    @patch('tap_dynamodb.sync_strategies.full_table.scan_table', return_value = {}) 
    def test_scan_table_with_nested_expr_with_list(self,mock_scan_table, mock_metadata_get, mock_get_bookmark, mock_write_bookmark, mock_write_state, mock_to_map):
        """Test expression attribute for multiple reserve word passed in `expression` field."""
        res = sync(CONFIG, STATE, STREAM)

        # Verify that `scan_table` called with expected paramater including nested expression attributes
        mock_scan_table.assert_called_with('GoogleDocs', '#tests.#x', {'#tests': 'tests', '#x': 'x'}, {}, {'start_date': '2017-01-01',
        'account_id': 'dummy_account_id', 'role_name': 'dummy_role', 'external_id': 'dummy_external_id', 'region_name': 'dummy_region_name'})
