import os
from flask import Flask, request, jsonify, g, abort
from http import HTTPStatus

from app.models import ShortMovie
from app.validators import SearchMoviesValidator, ValidationErrorResponse
from app.interface import MovieRegistry
from app.es_registry import ESMovieRegistry

es_host = os.environ.get('ES_HOST', 'localhost')
es_port = os.environ.get('ES_PORT', 9200)

app = Flask(__name__)


def get_registry() -> MovieRegistry:
    if 'registry' not in g:
        g.registry = ESMovieRegistry(es_host, es_port)
    return g.registry


@app.route('/api/movies', methods=['GET'])
def movies():
    form = SearchMoviesValidator(request.args)
    if not form.validate():
        return jsonify(ValidationErrorResponse(form.errors)), HTTPStatus.UNPROCESSABLE_ENTITY

    r = get_registry()
    movies = r.search_movies(form.data.get('search'), form.data.get('sort'),
                             form.data.get('sort_order'), form.data.get('limit'), form.data.get('page'))

    return jsonify(movies)


@app.route('/api/movies/', defaults={"movie_id": "tt0000410"}, methods=['GET'])
@app.route('/api/movies/<movie_id>', methods=['GET'])
def movie(movie_id):
    r = get_registry()
    movie = r.get_movie_by_id(movie_id)
    if movie is None:
        abort(HTTPStatus.NOT_FOUND)
    return jsonify(movie)


if __name__ == "__main__":
    app.run(port=8000, debug=True)
