from .interface import MovieRegistry
from .models import ShortMovie, Movie, Writer, Actor
from .validators import SortOrder, Sort

import requests
from typing import List, Optional


class ESMovieRegistry(MovieRegistry):
    """
    Registry containing movies
    """

    def __init__(self, es_host: str, es_port: int):
        self.es_url = f"http://{es_host}:{es_port}"

        self.session = requests.Session()
        self.session.headers.update({
            "Connection": "Keep-Alive",
            "Content-Type": "application/x-ndjson",
        })

    def get_movie_by_id(self, movie_id: str) -> Optional[Movie]:
        resp = self.session.get(f"{self.es_url}/movies/_doc/{movie_id}")
        data = resp.json()
        if not data["found"]:
            return None
        movie_raw = data["_source"]

        return Movie(
            id=movie_raw["id"],
            title=movie_raw["title"],
            description=movie_raw["description"],
            imdb_rating=float(movie_raw["imdb_rating"]),
            genre=movie_raw["genre"],
            director=movie_raw["director"],
            writers=[Writer(id=w["id"], name=w["name"])
                     for w in movie_raw["writers"]],
            actors=[Actor(id=int(a["id"]), name=a["name"])
                    for a in movie_raw["actors"]],
        )

    def search_movies(self,
                      search_query: str,
                      sort: str,
                      sort_order: str,
                      limit: int,
                      page: int) -> List[ShortMovie]:
        es_req = {
            "from": limit * (page - 1),
            "size": limit,
            "sort": [{f'{sort}.keyword': sort_order}],
            "_source": ["id", "title", "imdb_rating"],
        }

        if search_query:
            es_req["query"] = {
                "multi_match": {
                    "query": search_query,
                    "fuzziness": "auto",
                    "fields": [
                        "title^5",
                        "description^4",
                        "genre^3",
                        "actors_names^3",
                        "writers_names^2",
                        "director"
                    ]
                }
            }

        resp = self.session.get(f"{self.es_url}/movies/_search", json=es_req)
        if not resp.ok:
            resp.raise_for_status()
        data = resp.json()
        movies_found = data["hits"]["hits"]

        return [ShortMovie(id=m["_source"]["id"], title=m["_source"]["title"], imdb_rating=m["_source"]["imdb_rating"]) for m in movies_found]
