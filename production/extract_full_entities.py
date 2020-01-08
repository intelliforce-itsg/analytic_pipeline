
import mysql.connector
import pandas as pd
import os
# Retrieve set environment variables
PASSWORD = os.environ.get('NEWS_DB_PASSWORD')

def database_connect():
    cnx = mysql.connector.connect(
        host="localhost",
        database="news",
        user="whroe",
        passwd=PASSWORD
    )
    return cnx


def clean_data(data):
    if data is not None:
        if len(data.strip()) > 0:
            data = data.replace('b\'', '')
            data = data.replace('\"', '')
            data = data.replace('\'', '')
            data = data.replace('%', '')
            data = data.replace("xc3x83xc2xa2xc3x82xc2x80xc3x82xc2x99", "'")
            data = data.replace("xe2x80x99", "'")
            if data.startswith("b\'") or data.startswith("b'"):
                data = data[2:-1]
            data = data.strip()
        else:
            data = 'missing'
    else:
        data = 'missing'
    return data

entity_dictionary = {}

try:
    cnx = database_connect()
    cursor = cnx.cursor()

    topic_tag = 'FED_AI'

    # sql = 'select distinct(tag) from topic_tags'
    # cursor.execute(sql)
    # tags = cursor.fetchall()
    # for tag in tags:

    print(f'Processing topic {topic_tag}')

    # Build a list of article id's
    sql = f'select a.id, a.name, a.url, tt.tag from articles as a inner join topic_tags tt on a.id  = tt.article_id where tt.tag = \'{topic_tag}\' order by publish_date desc'

    # sql =   "select a.id, a.name, a.url, tt.tag " \
    #         "from articles a " \
    #         "inner join topic_tags tt on a.ID = tt.article_id " \
    #         "inner join named_entities ne on a.ID = ne.article_id " \
    #         "where tt.tag in ('TERROR', 'WAR') and named_entity like '%-per' and body like '%ISIS%' " \
    #         "order by publish_date desc, ne.article_id, ne.sentence_index, ne.word_index "

    cursor.execute(sql)
    articles = cursor.fetchall()
    for article in articles:
        id = article[0]
        name = clean_data(article[1])
        url = clean_data(article[2])
        tag = article[3]
        print(f'name: {name}')
        print(f'\turl: {url}')

        #sql = 'select named_entity, word, sentence_index, word_index, article_id from named_entities where named_entity like "%-org" order by article_id, sentence_index, word_index'
        sql = f'select named_entity, word, sentence_index, word_index, article_id from named_entities where article_id = {id} order by article_id, sentence_index, word_index'
        cursor.execute(sql)
        entities = cursor.fetchall()
        kb_persons = []
        last_article_id = None
        last_sentence_index = None
        last_word_index = None
        entity = ''
        entity_type = ''

        for part in entities:
            named_entity = part[0]
            word = part[1]
            sentence_index = part[2]
            word_index = part[3]
            article_id = part[4]

            if named_entity.startswith('B-'):
                entity_type = named_entity[2:]
                if len(entity.strip()) > 0:
                    print(f'\tENTITY {entity_type}: {entity}')
                    entity_dictionary[entity] = entity_type
                entity = ''
                entity = word + ' '
            elif article_id == last_article_id and last_sentence_index == sentence_index and word_index == last_word_index + 1:
                entity = entity + word + ' '
            else:
                if len(entity.strip()) > 0:
                    print(f'\tENTITY {entity_type}: {entity}')
                    entity_dictionary[entity] = entity_type
                entity = ''

            last_article_id = article_id
            last_sentence_index = sentence_index
            last_word_index = word_index

finally:
    cursor.close()
    cnx.close()


    print('Master list of entities:')
    for k in sorted(entity_dictionary.keys()):
        v = entity_dictionary[k]
        print(f'{k}:{v}')

