from importer.database import ArticleRepository
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
csv.write_rows(countries)
