from importer.database import ArticleRepository, PostRepository
from exporter.file import DelimitedFile


database = '/home/kobi/news_web_app/flaskblog/site.db'
print('Reading %s' % database)
articles = ArticleRepository(database)

print('Collecting countries ...')
countries = articles.get_countries()
print('Collected %d countries in %d seconds' % (len(countries), articles.process_time_in_sec))

file = 'countries.csv'
print('Writing %s' % file)
csv = DelimitedFile(file)
csv.write_row(('country',))
for country in countries:
    csv.write_row((country,))

for country in countries:
    print('Collecting articles for %s ...' % country)
    data = articles.get_data_from_country(country)
    print('Collected %d articles in %d seconds' % (len(data), articles.process_time_in_sec))

    file = 'articles_%s.csv' % country.lower()
    print('Writing %s' % file)
    csv = DelimitedFile(file)
    csv.write_row(tuple(articles.get_field_names(['country'])))
    csv.write_rows(data)
