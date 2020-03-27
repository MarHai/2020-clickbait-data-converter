from importer.database import ArticleRepository
from exporter.file import DelimitedFile


database = '/home/kobi/news_web_app/flaskblog/site.db'
print('Reading %s' % database)
articles = ArticleRepository(database)

for country in ['Australia', 'Canada', 'Ireland', 'New Zealand', 'United Kingdom',
                'United States', 'United States Minor Outlying Islands']:
    print('Collecting articles for %s ...' % country)
    data = articles.get_data_from_country(country)
    print('Collected %d articles in %d seconds' % (len(data), articles.process_time_in_sec))

    file = 'articles_%s.csv' % country.lower()
    print('Writing %s' % file)
    csv = DelimitedFile(file)
    csv.write_row(tuple(articles.get_field_names(['country'])))
    csv.write_rows(data)
