import csv
import json
import datetime
from typing import List


class DelimitedFile:
    def __init__(self, output_file: str):
        self._filename = output_file
        self._file = open(self._filename, 'w', newline='', encoding='utf-8')
        self._writer = csv.writer(self._file)

    def write_row(self, row: tuple):
        self._writer.writerow(row)

    def write_rows(self, rows: list):
        self._writer.writerows(rows)

    def __del__(self):
        self._file.close()


class JsonLines:
    def __init__(self, output_file: str):
        self._filename = output_file
        self._file = open(self._filename, 'w', newline='\n', encoding='utf-8')

    def write_row(self, id: str, timestamp: datetime, clickbait_text: List[str],
                  target_title: str = '', target_text: str = ''):
        """Follows specification from https://www.clickbait-challenge.org/clickbait17-dataset-schema.txt

        :param id:
        :param timestamp:
        :param clickbait_text:
        :param target_title:
        :param target_text:
        :return:
        """
        self._file.write(json.dumps({
            "id": id,
            "postTimestamp": timestamp.strftime('%a %b %d %H:%M:%S %z %Y'),
            "postText": clickbait_text,
            "postMedia": [],
            "targetTitle": target_title,
            "targetDescription": target_text,
            "targetKeywords": '',
            "targetParagraphs": [],
            "targetCaptions": []
        }))

    def write_rows(self, rows: List[dict]):
        for row in rows:
            self.write_row(**row)

    def __del__(self):
        self._file.close()
