import unittest

from unittest import mock
from tap_dynamodb import deserialize

class TestApplyProjection(unittest.TestCase):

    def test_project_expression1(self):
        '''
            Example Projection: Artist,metadata[0]
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata[0]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)


    def test_project_expression2(self):
        '''
            Example Projection: Artist,metadata[0],metadata[1]
            Stream Record: {'Artist': 'No One You Know','metadata': ['test1']}
        '''
        mock_record = {'Artist': 'No One You Know5','metadata': ['test1']}
        mock_projections =  [['Artist'], ['metadata[0]'], ['metadata[1]']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)


    def test_project_expression3(self):
        '''
            Example Projection: Artist,metadata[0].Age
            Stream Record: {'Artist': 'No One You Know'}
        '''
        mock_record = {'Artist': 'No One You Know5'}
        mock_projections =  [['Artist'], ['metadata[0]', 'Age']]

        deserializer = deserialize.Deserializer()
        output = deserializer.apply_projection(mock_record, mock_projections)
