import sqlite3
import time
from typing import List


class ArticleRepository:
    def __init__(self, database: str):
        self._db = sqlite3.connect(database)
        self._prepare_pragma()
        self._process_time_in_sec = 0.0
        self._fields = ['id', 'publisher', 'country',
                        'link', 'headline', 'excerpt', 'likes', 'comments', 'shares', 'twitter']

    def _set_process_time_in_sec(self, seconds: float):
        self._process_time_in_sec = seconds

    def _get_process_time_in_sec(self) -> float:
        return self._process_time_in_sec

    process_time_in_sec = property(_get_process_time_in_sec, _set_process_time_in_sec)

    def _prepare_pragma(self):
        self._db.execute('PRAGMA page_size = 4096')
        self._db.execute('PRAGMA cache_size=10000')
        self._db.execute('PRAGMA locking_mode=EXCLUSIVE')
        self._db.execute('PRAGMA temp_store = MEMORY')

    def _start_timer(self):
        self.process_time_in_sec = time.clock()

    def _stop_timer(self):
        self.process_time_in_sec = time.clock() - self.process_time_in_sec

    def get_countries(self) -> List[str]:
        countries = []
        self._start_timer()
        for row in self._db.execute('SELECT country FROM article WHERE country <> ""'):
            if row[0] not in countries:
                countries.append(row[0])
        self._stop_timer()
        return countries

    def get_articles_from_country(self, country: str) -> List[tuple]:
        fields = self._fields
        fields.remove('country')
        fields = ', '.join(fields)

        self._start_timer()
        articles = self._db.execute('SELECT %s FROM article WHERE country = ?' % fields, country).fetchall()
        self._stop_timer()

        return articles
