from abc import ABC, abstractmethod
from typing import List

from .validators import Sort, SortOrder
from .models import Movie, ShortMovie


class MovieRegistry(object):
    """
    Abstract class describing movie registry
    """

    def get_movie_by_id(self, id: str) -> Movie:
        raise NotImplementedError

    def search_movies(self,
                      search_query: str,
                      sort: Sort,
                      sort_order: SortOrder,
                      limit: int,
                      page: int) -> List[ShortMovie]:
        raise NotImplementedError
