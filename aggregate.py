# -*- coding: utf-8 -*-
"""Transform the huge country-specific CSVs into one even huger JSON-lines file ready for the Clickbait Challenge
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

articles = []
article_links = []
post_ids = []

for file_suffix in ['australia', 'canada', 'ireland', 'new-zealand', 'united-kingdom', 'united-states']:
    articles_file = 'articles_%s.csv' % file_suffix
    print('Collecting articles from %s ...' % articles_file)
    for article in DelimitedFile(articles_file):
        if article[2] not in article_links:
            article_links.append(article[2])
            articles.append((
                article[0],  # articleId
                file_suffix,  # articleCountry
                article[3],  # articleTitle
                article[4]  # articleDescription
            ))

instances_file = 'instances.jsonl'
print('Writing aggregated data into %s ...' % instances_file)
instances = JsonLines(instances_file)

for file_suffix in ['canada', 'ireland', 'united-kingdom', 'united-states', 'eu']:
    posts_file = 'posts_%s.csv' % file_suffix
    print('Collecting posts from %s ...' % posts_file)
    for post in DelimitedFile(posts_file):
        if post[0] not in post_ids:
            post_ids.append(post[0])
            for i, article_link in enumerate(article_links):
                if post[3] == article_link:
                    instances.write_row(
                        '%s_%s_%s_%s' % (articles[i][1], articles[i][0], file_suffix, post[0]),  # ID
                        datetime.strptime(post[6], '%Y-%m-%d %H:%M:%S.%f'),  # timestamp
                        [post[4], post[5]],  # clickbait text(s)
                        articles[i][2],  # target title
                        articles[i][3]  # target text
                    )

print('Done')
