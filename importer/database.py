import sqlite3
import time
from typing import List


class ArticleRepository:
    def __init__(self, database: str):
        self._db = sqlite3.connect(database)
        self._prepare_pragma()
        self._process_time_in_sec = 0.0
        self._table = 'article'
        self._fields = ['id', 'publisher', 'country',
                        'link', 'headline', 'excerpt', 'likes', 'comments', 'shares', 'twitter']

    def _get_table(self) -> str:
        return self._table

    name = property(_get_table)

    def _set_process_time_in_sec(self, seconds: float):
        self._process_time_in_sec = seconds

    def _get_process_time_in_sec(self) -> float:
        return self._process_time_in_sec

    process_time_in_sec = property(_get_process_time_in_sec, _set_process_time_in_sec)

    def _prepare_pragma(self):
        self._db.execute('PRAGMA page_size = 4096')
        self._db.execute('PRAGMA cache_size = 10000')
        self._db.execute('PRAGMA locking_mode = EXCLUSIVE')
        self._db.execute('PRAGMA temp_store = MEMORY')

    def _start_timer(self):
        self.process_time_in_sec = time.clock()

    def _stop_timer(self):
        self.process_time_in_sec = time.clock() - self.process_time_in_sec

    def get_field_names(self, exclude=None) -> List[str]:
        if exclude is None:
            exclude = []

        fields = self._fields.copy()
        for field in exclude:
            if field in fields:
                fields.remove(field)

        return fields

    def get_countries(self) -> List[str]:
        countries = []
        self._start_timer()
        for row in self._db.execute('SELECT country FROM %s WHERE country <> ""' % self._table):
            if row[0] not in countries:
                countries.append(row[0])
        self._stop_timer()
        return countries

    def get_data_from_country(self, country: str) -> List[tuple]:
        self._start_timer()
        sql = 'SELECT %s FROM %s WHERE country = "%s"' % (', '.join(self.get_field_names(['country'])),
                                                          self._table,
                                                          country)
        articles = self._db.execute(sql).fetchall()
        self._stop_timer()

        return articles


class PostRepository(ArticleRepository):
    def __init__(self, database: str):
        super().__init__(database)
        self._table = 'facebook'
        self._fields = ['id', 'page_name', 'country',
                        'external_link', 'post_type',
                        'headline', 'excerpt', 'publication_time',
                        'likes', 'comments', 'shares', 'loves', 'wows', 'hahas', 'sads', 'angrys']
