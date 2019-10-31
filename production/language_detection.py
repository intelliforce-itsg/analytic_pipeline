import spacy
from spacy_langdetect import LanguageDetector


import mysql.connector
import pandas as pd
import os
# Retrieve set environment variables
from tqdm import tqdm

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
        try:
            data = str(data.encode())
        except:
            pass
        data = data.replace("xc3x83xc2xa2xc3x82xc2x80xc3x82xc2x99", "'")
        data = data.replace("xe2x80x99", "'")
        data = data.replace('"', '\"')
        if data.startswith("b\'") or  data.startswith("b'"):
            data = data[2:-1]
        data = data.replace('\"', '')
        data = data.replace('\'', '')
        data = data.replace('%', '')
        data = data.strip()
    else:
        data = 'missing'
    return data #data.encode()

nlp = spacy.load('en')
nlp.add_pipe(LanguageDetector(), name='language_detector', last=True)

#try:
cnx = database_connect()
cursor = cnx.cursor()

# ideally the most recent -- or most text will be at the top
#    sql = 'select name, url, id, body from articles order by url, LENGTH(body) desc'
sql = 'select name, url, id, body, source, author from articles'


cursor.execute(sql)
articles = cursor.fetchall()
cursor.close()
X_train = []
print(len(articles))
print(f'Running language analytic on all articles')
for article in tqdm(articles):
    cursor = cnx.cursor()
    name  = clean_data(article[0])
    url = clean_data(article[1])
    id = article[2]
    body = clean_data(article[3])
    source = clean_data(article[4])
    author = clean_data(article[5])
    text = 'This is an english text.'
    doc = nlp(body)
    #{'language': 'en', 'score': 0.9999954772277541}
    language = doc._.language['language']
    score = doc._.language['score']
    try:
        sql = f'INSERT INTO marks (name, type, score, article_id) values ("{language}", "LID", "{score}", "{id}"  )'
        cursor.execute(sql)
        cnx.commit()
        cursor.close()
        #print(f'id:{id}, name: {name}, language: {doc._.language}')
    except Exception as e:
        print(e)

#finally:
cnx.close()

from sys import exit
exit(0)