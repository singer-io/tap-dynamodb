import unittest
from tap_dynamodb import deserialize

class TestDeserializer(unittest.TestCase):

    def test_projection_expression_1(self):
        '''
            Example Projection: Artist, metadata[0]
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata[0]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we get empty list if the data is not found in the record
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': []})


    def test_projection_expression_2(self):
        '''
            Example Projection: Artist, metadata[0], metadata[1]
            Stream Record: {'Artist': 'No One You Know','metadata': ['test1']}
        '''
        mock_record = {'Artist': 'No One You Know5','metadata': ['test1']}
        mock_projections =  [['Artist'], ['metadata[0]'], ['metadata[1]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we only get the available data in the list when user expect the data that is not available
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': ['test1']})


    def test_projection_expression_3(self):
        '''
            Example Projection: Artist, metadata[0].Age
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata[0]', 'Age']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # verify that we get empty dict when the element in the list is parent element and parent is not found
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': [{}]})

    def test_projection_expression_4(self):
        '''
            Example Projection: Artist, metadata.inner_metadata
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata', 'inner_metadata']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
        # veriy that we get None when the parent data is not found
        self.assertEquals(output, {'Artist': 'No One You Know5', 'metadata': {'inner_metadata': None}})
