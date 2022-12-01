import unittest
from tap_dz_dynamodb import deserialize

class TestDeserializer(unittest.TestCase):

    def test_projection_expression_all_list_data_not_found(self):
        '''
            Verify that we get empty list if the data is not found in the record
            Example Projection: Artist, metadata[0]
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata[0]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we get empty list if the data is not found in the record
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': []})

    def test_projection_expression_all_list_data_not_found_positive(self):
        '''
            Verify that we get empty list if the data is not found in the record
            Example Projection: Artist, metadata[0]
            Stream Record: {'Artist': 'No One You Know5', 'metadata': ['test']}
        '''
        mock_record = {'Artist': 'No One You Know5', 'metadata': ['test']}
        mock_projections =  [['Artist'], ['metadata[0]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we get empty list if the data is not found in the record
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': ['test']})

    def test_projection_expression_some_list_data_not_found(self):
        '''
            Verify that we only get the available data in the list when user expect the data that is not available
            Example Projection: Artist, metadata[0], metadata[1]
            Stream Record: {'Artist': 'No One You Know','metadata': ['test1']}
        '''
        mock_record = {'Artist': 'No One You Know5','metadata': ['test1']}
        mock_projections =  [['Artist'], ['metadata[0]'], ['metadata[1]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we only get the available data in the list when user expect the data that is not available
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': ['test1']})

    def test_projection_expression_some_list_data_not_found_positive(self):
        '''
            Verify that we only get the available data in the list when user expect the data that is not available
            Example Projection: Artist, metadata[0], metadata[1]
            Stream Record: {'Artist': 'No One You Know','metadata': ['test1', 'test2']}
        '''
        mock_record = {'Artist': 'No One You Know5','metadata': ['test1', 'test2']}
        mock_projections =  [['Artist'], ['metadata[0]'], ['metadata[1]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we only get the available data in the list when user expect the data that is not available
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': ['test1', 'test2']})

    def test_projection_expression_parent_child_data_list(self):
        '''
            Verify that we get empty dict when the element in the list is parent element and it is not found
            Example Projection: Artist, metadata[0].Age
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata[0]', 'Age']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we get empty dict when the element in the list is parent element and it is not found
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': [{}]})

    def test_projection_expression_parent_child_data_list_positive(self):
        '''
            Verify that we get empty dict when the element in the list is parent element and it is not found
            Example Projection: Artist, metadata[0].Age
            Stream Record: {'Artist': 'No One You Know5', 'metadata': [{'Age': 'Test'}]}
        '''
        mock_record = {'Artist': 'No One You Know5', 'metadata': [{'Age': 'Test'}]}
        mock_projections =  [['Artist'], ['metadata[0]', 'Age']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we get empty dict when the element in the list is parent element and it is not found
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': [{'Age': 'Test'}]})

    def test_projection_expression_parent_child_data_dictionary(self):
        '''
            Veriy that we get None when the parent data is not found and we are requesting for child data
            Example Projection: Artist, metadata.inner_metadata
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata', 'inner_metadata']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # veriy that we get None when the parent data is not found
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': {}})

    def test_projection_expression_parent_child_data_dictionary_positive(self):
        '''
            Veriy that we get None when the parent data is not found and we are requesting for child data
            Example Projection: Artist, metadata.inner_metadata
            Stream Record: {'Artist': 'No One You Know5', 'metadata': {'inner_metadata': 'Test'}}
        '''
        mock_record = {'Artist': 'No One You Know5', 'metadata': {'inner_metadata': 'Test'}}
        mock_projections =  [['Artist'], ['metadata', 'inner_metadata']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # veriy that we get None when the parent data is not found
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': {'inner_metadata': 'Test'}})

    def test_projection_expression_parent_child_data_list_different_order(self):
        '''
            Veriy that we get no error when add list projection in decreasing order and it is not found
            Example Projection: metadata[1], metadata[0].inner_metadata
            Stream Record: {'metadata': [{'inner_metadata': 'Test'}]}
        '''
        mock_record = {'metadata': [{'inner_metadata': 'Test'}]}
        mock_projections =  [['metadata[1]'], ['metadata[0]', 'inner_metadata']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # veriy that we get no error when add list projection in decreasing order and it is not found
        self.assertEquals(output, {'metadata': [{'inner_metadata': 'Test'}]})
