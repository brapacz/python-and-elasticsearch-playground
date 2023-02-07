import unittest
from elasticsearch import Elasticsearch
from elasticsearch import exceptions

class ElasticMixin:
    def setUp(self):
        self.client = Elasticsearch("http://localhost:9200")
        self.client.indices.delete(index='_all')

    def tearDown(self):
        pass

class TestElasticQueries(ElasticMixin, unittest.TestCase):

    def test_simple_query(self):
        self.client.index(
             index='lord-of-the-rings',
             id='aragorn',
             body={
                'character': 'Aragon',
                'quote': 'It is not this day.'
             }
        )
        self.client.indices.refresh()
        result = self.client.search(
            index='lord-of-the-rings',
            body={
                'query': {
                    'match': {'quote': 'day'}
                }
            }
        )
        print(result['hits']['hits'])

if __name__ == '__main__':
    unittest.main()
