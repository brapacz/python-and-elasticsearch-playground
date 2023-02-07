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

    def setUp(self):
        super().setUp()
        # single artist with ID
        self.client.index(
             index='songs',
             id='numb',
             body={
                'title': 'Numb',
                'artists': [
                    {'name': 'Linkin Park', 'id': 'lp'}
                ]
             }
        )

        # some artists have id
        self.client.index(
             index='songs',
             id='numb-encore',
             body={
                'title': 'Numb Encore',
                'artists': [
                    {'name': 'Linkin Park', 'id': 'lp'},
                    {'name': 'Jay-Z'},
                ]
             }
        )

        # all artists with id
        self.client.index(
             index='songs',
             id='lying-from-you',
             body={
                'title': 'Lying From You',
                'artists': [
                    {'name': 'Linkin Park', 'id': 'lp'},
                    {'name': 'Eminem', 'id': 'emn'},
                ]
             }
        )

        # only artist without id
        self.client.index(
             index='songs',
             id='99-problems',
             body={
                'title': '99 Problems',
                'artists': [
                    {'name': 'Jay-Z'},
                ]
             }
        )

        # no artist
        self.client.index(
             index='songs',
             id='moonlight-sonata',
             body={
                'title': 'Moonlight sonata',
                'artists': []
             }
        )

        self.client.indices.refresh()


    def test_simple_query(self):
        result = self.client.search(
            index='songs',
            body={
                'query': {
                    'match': {'title': 'numb'}
                }
            }
        )
        hits = [hit['_id'] for hit in result['hits']['hits']]
        self.assertEqual(hits, ['numb', 'numb-encore'])

if __name__ == '__main__':
    unittest.main()
