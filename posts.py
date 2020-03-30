# -*- coding: utf-8 -*-
"""Transform the database into huge country-specific CSVs"""

from importer.database import PostRepository
from exporter.file import DelimitedFile

database = '/home/kobi/news_web_app/flaskblog/site.db'
print('Reading %s' % database)
posts = PostRepository(database)

for country in ['Canada', 'Ireland', 'United Kingdom', 'United States', 'EU']:
    print('Collecting posts for %s ...' % country)
    data = posts.get_data_from_country(country)
    print('Collected %d posts in %d seconds' % (len(data), posts.process_time_in_sec))

    file = 'posts_%s.csv' % country.lower()
    print('Writing %s' % file)
    csv = DelimitedFile(file)
    csv.write_row(tuple(posts.get_field_names(['country'])))
    csv.write_rows(data)
