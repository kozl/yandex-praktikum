from wtforms import Form, IntegerField, SelectField, StringField
from wtforms import validators
from enum import Enum
from dataclasses import dataclass
from typing import Dict, List


class SortOrder(Enum):
    ASC = 'asc'
    DESC = 'desc'


class Sort(Enum):
    ID = 'id'
    TITLE = 'title'
    IMDB_RATING = 'imdb_rating'


@dataclass(init=False)
class ValidationErrorResponse:
    detail: List[Dict]

    def __init__(self, validation_errors):
        self.detail = []
        for field_name, field_errors in validation_errors.items():
            for err in field_errors:
                self.detail.append({
                    "loc": [
                        "query",
                        field_name
                    ],
                    "msg": err,
                })


class SearchMoviesValidator(Form):
    limit = IntegerField(u'limit', validators=[
                         validators.NumberRange(min=0)], default=50)
    page = IntegerField(u'page', validators=[
                        validators.NumberRange(min=1)], default=1)
    sort = SelectField(u'sort', choices=[
                       Sort.ID.value, Sort.TITLE.value, Sort.IMDB_RATING.value], default=Sort.ID.value)
    sort_order = SelectField(u'sort_order', choices=[
        SortOrder.ASC.value, SortOrder.DESC.value], default=SortOrder.ASC.value)
    search = StringField(u'search', default='')
