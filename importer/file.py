# -*- coding: utf-8 -*-
"""Read CSV files"""

import csv
from typing import List


class DelimitedFile:
    def __init__(self, input_file: str):
        self._filename = input_file
        self._file = open(self._filename, 'r', newline='', encoding='utf-8')
        self._reader = csv.reader(self._file)
        self._header = []
        self.header = next(self._reader)

    def _set_header(self, row: List[str]):
        self._header = row

    def _get_header(self) -> List[str]:
        return self._header

    header = property(_get_header, _set_header)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._reader)

    def __del__(self):
        self._file.close()
