from dataclasses import dataclass, asdict, field
from typing import List


@dataclass
class Actor(object):
    id: int
    name: str


@dataclass
class Writer(object):
    id: int
    name: str


@dataclass
class ShortMovie:
    id: str
    title: str
    imdb_rating: float = 0.0


@dataclass
class Movie(ShortMovie):
    description: str = ""
    genre: List[str] = field(default_factory=list)
    director: List[str] = field(default_factory=list)
    writers: List[Writer] = field(default_factory=list)
    actors: List[Actor] = field(default_factory=list)
