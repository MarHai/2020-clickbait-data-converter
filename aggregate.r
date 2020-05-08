library(dplyr)
library(magrittr)
library(readr)
library(stringr)


articles <- read_csv('articles_australia.csv', col_types = 'dccccdddd') %>% mutate(country = 'AU')
articles <- articles %>% bind_rows(read_csv('articles_canada.csv', col_types = 'dccccdddd') %>% mutate(country = 'CA'))
articles <- articles %>% bind_rows(read_csv('articles_ireland.csv', col_types = 'dccccdddd') %>% mutate(country = 'IR'))
articles <- articles %>% bind_rows(read_csv('articles_new-zealand.csv', col_types = 'dccccdddd') %>% mutate(country = 'NZ'))
articles <- articles %>% bind_rows(read_csv('articles_united-kingdom.csv', col_types = 'dccccdddd') %>% mutate(country = 'UK'))
articles <- articles %>% bind_rows(read_csv('articles_united-states.csv', col_types = 'dccccdddd') %>% mutate(country = 'US'))
articles <- articles %>% bind_rows(read_csv('articles_united-states-minor-outlying-islands.csv', col_types = 'dccccdddd') %>% mutate(country = 'US'))
articles <- 
  articles %>% 
  distinct(link, .keep_all = TRUE) %>% 
  select(article_id = id, link, country, headline, excerpt)

posts <- read_csv('posts_canada.csv', col_types = 'dcccccTdddddddd')
posts <- posts %>% bind_rows(read_csv('posts_eu.csv', col_types = 'dcccccTdddddddd'))
posts <- posts %>% bind_rows(read_csv('posts_ireland.csv', col_types = 'dcccccTdddddddd'))
posts <- posts %>% bind_rows(read_csv('posts_united-kingdom.csv', col_types = 'dcccccTdddddddd'))
posts <- posts %>% bind_rows(read_csv('posts_united-states.csv', col_types = 'dcccccTdddddddd'))
posts <- 
  posts %>% 
  distinct(id, .keep_all = TRUE) %>% 
  select(post_id = id, link = external_link, publication_time, headline, excerpt)

lct <- Sys.getlocale('LC_TIME')
Sys.setlocale('LC_TIME', 'en_US.UTF-8')

step <- 1000000
num_rows <- nrow(articles)
for(row in seq(1, num_rows, step)) {
  last_row <- row + step - 1
  if(last_row > num_rows) {
    last_row <- num_rows
  }
  articles_temp <- articles[row:last_row,]
  articles_temp %>%
    left_join(posts, by = 'link', suffix = c('.article', '.post')) %>%
    filter(!is.na(post_id))
  articles_temp %>%
    mutate(
      id = str_glue('{article_id}_{post_id}'),
      postTimestamp = format(publication_time, '%a %b %d %H:%M:%S %z %Y'),
      headline.post = str_replace_all(headline.post, '"', '\\\\"'),
      excerpt.post = str_replace_all(excerpt.post, '"', '\\\\"'),
      headline.article = str_replace_all(headline.article, '"', '\\\\"'),
      excerpt.article = str_replace_all(excerpt.article, '"', '\\\\"'),
      json = str_glue('{{"id":"{id}",',
                      '"postTimestamp":"{postTimestamp}",',
                      '"postText":["{headline.post}","{excerpt.post}"],',
                      '"postMedia":[],',
                      '"targetTitle":"{headline.article}",',
                      '"targetDescription":"{excerpt.article}",',
                      '"targetKeywords":"",',
                      '"targetParagraphs":[],',
                      '"targetCaptions":[]}}')
    ) %>%
    pull(json) %>%
    write_lines('instances.jsonl',
                append = TRUE)
}

Sys.setlocale('LC_TIME', lct)
