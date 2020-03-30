# -*- coding: utf-8 -*-
"""Extract lists of unique countries from the database for both articles and posts"""

from importer.database import ArticleRepository, PostRepository
from exporter.file import DelimitedFile

database = '/home/kobi/news_web_app/flaskblog/site.db'
print('Reading %s' % database)

for repository in [ArticleRepository(database), PostRepository(database)]:
    print('Collecting countries for %s ...' % repository.name)

    countries = repository.get_countries()
    print('Collected %d %s countries in %d seconds' % (len(countries), repository.name, repository.process_time_in_sec))

    file = '%s_countries.csv' % repository.name
    print('Writing %s' % file)
    csv = DelimitedFile(file)
    csv.write_row(('country',))
    for country in countries:
        csv.write_row((country,))
