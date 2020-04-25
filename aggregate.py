# -*- coding: utf-8 -*-
"""Transform the huge country-specific CSVs into one even huger JSON-lines file ready for the Clickbait Challenge
0. create a RAM-based sqlite database to handle everything memory-efficient
1. collect all articles from all countries, unique on article link
2. run through all posts from all countries but handle each post (based on post ID) only once
3a. per post, find all articles linked to,
 b. create a new ID (articleCountry_articleId_postCountry_postId),
 c. collect the postTimestamp, postHeadline and postText, as well as articleHeadline and articleText, and
 d. append a JSON line
"""

from importer.file import DelimitedFile
from exporter.file import JsonLines
from datetime import datetime
import sqlite3

article_links = []
post_ids = []

db = sqlite3.connect(':memory:')
db.execute('create table article ('
           'uid int(11) constraint article_pk primary key, '
           'country varchar(50), '
           'title text, '
           'description text, '
           'url text not null constraint url unique'
           ');')
db.execute('create unique index article_uid_uindex on article (uid);')

for file_suffix in ['australia', 'canada', 'ireland', 'new-zealand', 'united-kingdom', 'united-states',
                    'united-states-minor-outlying-islands']:
    articles_file = 'articles_%s.csv' % file_suffix
    print('Collecting articles from %s ...' % articles_file)
    for article in DelimitedFile(articles_file):
        if article[2] not in article_links:
            article_links.append(article[2])
            db.execute('insert into article (uid, country, title, description, url) VALUES ({}, {}, {}, {}, {})'.format(
                article[0],  # articleId
                file_suffix,  # articleCountry
                article[3],  # articleTitle
                article[4],  # articleDescription
                article[2]  # articleUrl
            ))

print('%d articles collected' % db.execute('select count(*) from article').fetchone()[0])

instances_file = 'instances.jsonl'
print('Writing aggregated data into %s ...' % instances_file)
instances = JsonLines(instances_file)

for file_suffix in ['canada', 'ireland', 'united-kingdom', 'united-states', 'eu']:
    posts_file = 'posts_%s.csv' % file_suffix
    print('Collecting posts from %s ...' % posts_file)
    for post in DelimitedFile(posts_file):
        if post[0] not in post_ids:
            post_ids.append(post[0])
            article = db.execute('select * from article where url = {}'.format(post[3])).fetchone()
            if article is not None:
                instances.write_row(
                    '%s_%s_%s_%s' % (article[1], article[0], file_suffix, post[0]),  # ID
                    datetime.strptime(post[6], '%Y-%m-%d %H:%M:%S.%f'),  # timestamp
                    [post[4], post[5]],  # clickbait text(s)
                    article[2],  # target title
                    article[3]  # target text
                )

print('Done')
