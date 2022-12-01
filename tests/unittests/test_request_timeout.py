from tap_dz_dynamodb.sync_strategies import full_table, log_based
from tap_dz_dynamodb import discover, dynamodb
import tap_dz_dynamodb
import unittest
from unittest import mock
from botocore.exceptions import ConnectTimeoutError, ReadTimeoutError

class MockClient():
    def __init__(self,endpoint_url=None):
        self.endpoint_url="abc.com"

    def list_tables(self, *args):
        raise ReadTimeoutError(endpoint_url="abc.com")

    def scan(self, **args):
        raise ReadTimeoutError(endpoint_url="abc.com")

    def describe_table(self, **args):
        raise ReadTimeoutError(endpoint_url="abc.com")

def mock_get_client(*args):
    return MockClient()
class MockClientConnectTimeout():
    def __init__(self,endpoint_url=None):
        self.endpoint_url="abc.com"

    def list_tables(self, *args):
        raise ConnectTimeoutError(endpoint_url="abc.com")

    def scan(self, **args):
        raise ConnectTimeoutError(endpoint_url="abc.com")

    def describe_table(self, **args):
        raise ConnectTimeoutError(endpoint_url="abc.com")

def mock_get_client(*args):
    return MockClient()

def mock_get_client_connect_timeout(*args):
    return MockClientConnectTimeout()

class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    @mock.patch('tap_dz_dynamodb.dynamodb.get_client',side_effect=mock_get_client)
    def test_discover_stream_read_timeout_and_backoff(self, mock_client):
        """
        Check whether the request backoffs properly for discover_streams for 5 times in case of ReadTimeoutError error.
        """
        with self.assertRaises(ReadTimeoutError):
            discover.discover_streams({"region_name": "dummy", "use_local_dynamo": "true"})
        self.assertEquals(mock_client.call_count, 5)

    @mock.patch('tap_dz_dynamodb.dynamodb.get_client',side_effect=mock_get_client)
    def test_scan_table_sync_read_timeout_and_backoff(self, mock_client):
        """
        Check whether the request backoffs properly for full_table sync for 5 times in case of ReadTimeoutError error.
        """
        with self.assertRaises(ReadTimeoutError):
            full_table.sync({"region_name": "dummy", "use_local_dynamo": "true"}, {}, {"tap_stream_id":"dummy_stream", "metadata": ""})
        self.assertEqual(mock_client.call_count, 5)

    @mock.patch('tap_dz_dynamodb.dynamodb.get_client',side_effect=mock_get_client)
    @mock.patch('tap_dz_dynamodb.dynamodb.get_stream_client',side_effect=mock_get_client)
    def test_get_records_sync_read_timeout_and_backoff(self, mock_stream_client, mock_client):
        """
        Check whether the request backoffs properly for log_based sync for 5 times in case of ReadTimeoutError error.
        """
        with self.assertRaises(ReadTimeoutError):
            log_based.sync({"region_name": "dummy", "use_local_dynamo": "true"}, {}, {"tap_stream_id":"dummy_stream", "metadata": ""})
        self.assertEqual(mock_client.call_count, 5)
        self.assertEqual(mock_stream_client.call_count, 5)

    @mock.patch('tap_dz_dynamodb.dynamodb.get_client',side_effect=mock_get_client_connect_timeout)
    def test_discover_stream_connect_timeout_and_backoff(self, mock_client):
        """
        Check whether the request backoffs properly for discover_streams for 5 times in case of ConnectTimeoutError error.
        """
        with self.assertRaises(ConnectTimeoutError):
            discover.discover_streams({"region_name": "dummy", "use_local_dynamo": "true"})
        self.assertEquals(mock_client.call_count, 5)

    @mock.patch('tap_dz_dynamodb.dynamodb.get_client',side_effect=mock_get_client_connect_timeout)
    def test_scan_table_sync_connect_timeout_and_backoff(self, mock_client):
        """
        Check whether the request backoffs properly for full_table sync for 5 times in case of ConnectTimeoutError error.
        """
        with self.assertRaises(ConnectTimeoutError):
            full_table.sync({"region_name": "dummy", "use_local_dynamo": "true"}, {}, {"tap_stream_id":"dummy_stream", "metadata": ""})
        self.assertEqual(mock_client.call_count, 5)

    @mock.patch('tap_dz_dynamodb.dynamodb.get_client',side_effect=mock_get_client_connect_timeout)
    @mock.patch('tap_dz_dynamodb.dynamodb.get_stream_client',side_effect=mock_get_client_connect_timeout)
    def test_get_records_sync_connect_timeout_and_backoff(self, mock_stream_client, mock_client):
        """
        Check whether the request backoffs properly for log_based sync for 5 times in case of ConnectTimeoutError error.
        """
        with self.assertRaises(ConnectTimeoutError):
            log_based.sync({"region_name": "dummy", "use_local_dynamo": "true"}, {}, {"tap_stream_id":"dummy_stream", "metadata": ""})
        self.assertEqual(mock_client.call_count, 5)
        self.assertEqual(mock_stream_client.call_count, 5)

    @mock.patch('tap_dz_dynamodb.dynamodb.get_client',side_effect=mock_get_client)
    def test_get_initial_bookmarks_read_timeout_and_backoff(self, mock_client):
        """
        Check whether the request backoffs properly for discover_streams for 5 times in case of ReadTimeoutError error.
        """
        with self.assertRaises(ReadTimeoutError):
            log_based.get_initial_bookmarks({"region_name": "dummy", "use_local_dynamo": "true"}, {}, "dummy_table")
        self.assertEquals(mock_client.call_count, 5)

class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''
    default_timeout_value = 300
    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_config_provided_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on config value
        """
        timeout_value = 100
        config = {"region_name": "dummy_region", "request_timeout": timeout_value}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=timeout_value, read_timeout=timeout_value)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_default_value_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"region_name": "dummy_region"}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=self.default_timeout_value, read_timeout=self.default_timeout_value)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_config_provided_empty_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"region_name": "dummy_region", "request_timeout": ""}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=self.default_timeout_value, read_timeout=self.default_timeout_value)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_config_provided_string_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"region_name": "dummy_region", "request_timeout": "100"}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=100, read_timeout=100)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_config_provided_float_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on config float value
        """
        timeout_value = 100.8
        config = {"region_name": "dummy_region", "request_timeout": timeout_value}
        dynamodb.get_client(config)
        mock_config.assert_called_with(connect_timeout=timeout_value, read_timeout=timeout_value)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_stream_config_provided_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on config value
        """
        timeout_value = 100
        config = {"region_name": "dummy_region", "request_timeout": timeout_value}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=timeout_value, read_timeout=timeout_value)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_stream_default_value_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"region_name": "dummy_region"}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=self.default_timeout_value, read_timeout=self.default_timeout_value)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_stream_config_provided_empty_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"region_name": "dummy_region", "request_timeout": ""}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=self.default_timeout_value, read_timeout=self.default_timeout_value)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_stream_config_provided_string_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"region_name": "dummy_region", "request_timeout": "100"}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=100, read_timeout=100)

    @mock.patch('boto3.client')
    @mock.patch("tap_dz_dynamodb.dynamodb.Config")
    def test_stream_config_provided_float_request_timeout(self, mock_config, mock_client):
        """
            Unit tests to ensure that request timeout is set based on config float value
        """
        timeout_value = 100.8
        config = {"region_name": "dummy_region", "request_timeout": timeout_value}
        dynamodb.get_stream_client(config)
        mock_config.assert_called_with(connect_timeout=timeout_value, read_timeout=timeout_value)