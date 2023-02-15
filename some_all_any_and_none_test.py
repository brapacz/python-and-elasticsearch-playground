import unittest
from elasticsearch import Elasticsearch
from elasticsearch import exceptions

class ElasticMixin:
    def setUp(self):
        self.client = Elasticsearch("http://localhost:9200")
        self.client.indices.delete(index='_all')
        self.songs = {}

    def tearDown(self):
        pass

class TestElasticQueries(ElasticMixin, unittest.TestCase):
    def extract_song_ids(self, result):
        return [hit['_id'] for hit in result['hits']['hits']]

    def setUp(self):
        super().setUp()
        # single artist with ID
        self.songs['numb'] = {
            'title': 'Numb',
            'artists': [
                {'name': 'Linkin Park', 'id': 'lp'}
            ]
        }

        # some artists have id
        self.songs['numb-encore'] = {
            'title': 'Numb Encore',
            'artists': [
                {'name': 'Linkin Park', 'id': 'lp'},
                {'name': 'Jay-Z'},
            ]
        }

        # all artists with id
        self.songs['lying-from-you'] = {
            'title': 'Lying From You',
            'artists': [
                {'name': 'Linkin Park', 'id': 'lp'},
                {'name': 'Eminem', 'id': 'emn'},
            ]
        }

        # only artist without id
        self.songs['99-problems'] = {
            'title': '99 Problems',
            'artists': [
                {'name': 'Jay-Z'},
            ]
        }

        # only artist with empty string as id
        self.songs['99-problems-null'] = {
            'title': '99 Problems',
            'artists': [
                {'name': 'Jay-Z', 'id': ''},
            ]
        }

        # no artist
        self.songs['moonlight-sonata'] = {
            'title': 'Moonlight sonata',
            'artists': []
        }

        for id, body in self.songs.items():
            self.client.index(index='songs', id=id, body=body)
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
        hits = self.extract_song_ids(result)
        self.assertEqual(['numb', 'numb-encore'], hits)

    def test_get_all_songs(self):
        result = self.client.search(
            index='songs',
            body={
                'query': {
                    'bool': {}
                }
            }
        )
        hits = self.extract_song_ids(result)
        self.assertEqual(list(self.songs.keys()), hits)

    def test_exclude_songs_without_any_artists(self):
        result = self.client.search(
            index='songs',
            body={
                'query': {
                    'bool': {
                        'must': [
                            {'range': { 'artists_count.total': { 'gt': 0 } }},
                        ]
                    }
                },
                'runtime_mappings': self.get_runtime_mappings(),
            }
        )
        hits = self.extract_song_ids(result)
        self.assertEqual(['numb', 'numb-encore', 'lying-from-you', '99-problems', '99-problems-null'], hits)


    def test_exclude_songs_when_none_of_the_artists_have_id(self):
        result = self.client.search(
            index='songs',
            body={
                'query': {
                    'bool': {
                        'must': [
                            { 'exists': {'field': 'artists.id' } },
                            { 'regexp': {'artists.id': '.+'} },
                        ]
                    }
                }
            }
        )
        hits = self.extract_song_ids(result)
        self.assertEqual(['numb', 'numb-encore', 'lying-from-you'], hits)

    def test_exclude_songs_when_any_artists_does_not_have_id(self):
        result = self.client.search(
            index='songs',
            body={
                'query': {
                    'bool': {
                        'must': [
                            {'range': { 'artists_count.total': { 'gt': 0 } }},
                            {'match': { 'artists_count.without_id': 0 }}
                        ]
                    }
                },
                'runtime_mappings': self.get_runtime_mappings(),
            }
        )
        hits = self.extract_song_ids(result)
        self.assertEqual(['numb', 'lying-from-you'], hits)

    def get_runtime_mappings(self):

        script = """
            int with_id = 0;
            int without_id = 0;
            int total = params['_source']['artists'].size();

            for(def artist : params['_source']['artists']) {
                if(null == artist['id'] || '' == artist['id']) {
                    without_id++;
                } else {
                    with_id++;
                }
            }

            emit('with_id', with_id);
            emit('without_id', without_id);
            emit('total', total);
        """

        return {
            "artists_count": {
                "type": "composite",
                "script": {
                    'source': script
                },
                "fields": {
                    "with_id": { "type": "long" },
                    "without_id": { "type": "long" },
                    "total": { "type": "long" },
                }
            }
        }


if __name__ == '__main__':
    unittest.main()
