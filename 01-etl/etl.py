import sqlite3
import requests
import json
import logging

from urllib.parse import urljoin
from typing import List

logger = logging.getLogger()

class ESLoader:
    def __init__(self, es_url: str = 'http://localhost:9200'):
        session = requests.Session()
        session.headers= {
            'Connection': 'Keep-Alive',
            'Content-Type': 'application/x-ndjson',
        }

        self.session = session
        self.url = es_url

    def _prepare_bulk_query(self, records: List[dict], index_name: str) -> str:
        query = []
        for record in records:
            query.extend([
                json.dumps({ 'index': { '_index': index_name, '_id': record['id'] } }),
                json.dumps(record),
            ])
        return '\n'.join(query) + '\n'

    def load(self, records: List[dict], index_name: str):
        query = self._prepare_bulk_query(records, index_name)
        response = self.session.post(
            urljoin(self.url, '_bulk'),
            data=query,
            timeout=3.05,
        )

        json_response = json.loads(response.text)
        if json_response['errors']:
            for item in json_response['items']:
                error_message = item.get('error')
                if error_message:
                    logger.error(f'Error loading row {item["_id"]}: {error_message}')


class ETL:
    SQL = '''
    WITH x AS (
        SELECT m.id, group_concat(a.name) as actors_names, group_concat(a.id) as actors_ids
        FROM movies m
            JOIN movie_actors ma ON ma.movie_id = m.id
            JOIN actors a ON a.id = ma.actor_id
        GROUP BY ma.movie_id
    )

    SELECT 
        m.id,
        title,
        plot as description,
        director,
        genre, 
        imdb_rating,
        x.actors_names,
        x.actors_ids,
        CASE
            WHEN m.writers = '' THEN '[{"id": "' || m.writer || '"}]'
            ELSE m.writers
        END AS writers
        FROM movies m
        JOIN x ON x.id = m.id
    '''

    def __init__(self, conn: sqlite3.Connection, es_loader: ESLoader):
        def dict_factory(cursor: sqlite3.Cursor, row: sqlite3.Row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
        conn.row_factory = dict_factory

        self.sqlite = conn
        self.loader = es_loader
    
    def _get_writers(self) -> dict:
        writers = {}
        for writer in self.sqlite.execute('''SELECT DISTINCT id, name FROM writers'''):
            writers[writer['id']] = writer
        return writers

    def extract(self) -> List[dict]:
        records = []
        for row in self.sqlite.execute(self.SQL):
            records.append(row)
        
        return records

    def transform(self, records: List[dict]) -> List[dict]:
        transformed_records = []
        writers = self._get_writers()
        for record in records:
            movie_writers = []
            movie_writers_set = set()
            for writer in json.loads(record['writers']):
                writer_id = writer['id']
                if writers.get(writer_id) and writers[writer_id]['name'] != 'N/A' and writer_id not in movie_writers_set:
                    movie_writers.append(writers[writer_id])
                    movie_writers_set.add(writer_id)
            
            actors_names = []
            actors = []
            if record['actors_names'] is not None and record['actors_ids'] is not None:
                actors = [{'id': _id, 'name': name}
                          for _id, name in zip(record['actors_ids'].split(','), record['actors_names'].split(',')) if name != 'N/A']
                actors_names = [
                    x for x in record['actors_names'].split(',') if x != 'N/A']

            transformed_records.append(
                {
                    'id': record['id'],
                    'imdb_rating': float(record['imdb_rating']) if record['imdb_rating'] != 'N/A' else None,
                    'director': [x.strip() for x in record['director'].split(',')] if record['director'] is not None and record['director'] != 'N/A' else None,
                    'genre': record['genre'].replace(' ', '').split(','),
                    'title': record['title'],
                    'actors': actors,
                    'actors_names': actors_names,
                    'writers': movie_writers,
                    'writers_names': [ x['name'] for x in movie_writers],
                    'description': record['description'] if record['description'] != 'N/A' else None
                }
            )
        return transformed_records


    def load(self, records: List[dict]):
        self.loader.load(records, 'movies')

if __name__ == '__main__':
    conn = sqlite3.connect('db.sqlite')
    etl = ETL(conn, ESLoader())
    
    records = etl.extract()
    transformed = etl.transform(records)
    etl.load(transformed)

