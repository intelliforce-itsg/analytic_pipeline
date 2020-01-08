import os

import mysql.connector
import spacy
from spacy_langdetect import LanguageDetector
from tqdm import tqdm

PASSWORD = os.environ.get('NEWS_DB_PASSWORD')

nlp = spacy.load('en_core_web_sm')
nlp.add_pipe(LanguageDetector(), name='language_detector', last=True)
spacy.prefer_gpu()


class LanguageDetection():
    def __init__(self):
        pass

    def database_connect(self):
        cnx = mysql.connector.connect(
            host="localhost",
            database="news",
            user="whroe",
            passwd=PASSWORD
        )
        return cnx

    def clean_data(self, data):
        if data is not None:
            try:
                data = str(data.encode())
            except:
                pass
            data = data.replace("xc3x83xc2xa2xc3x82xc2x80xc3x82xc2x99", "'")
            data = data.replace("xe2x80x99", "'")
            data = data.replace('"', '\"')
            if data.startswith("b\'") or data.startswith("b'"):
                data = data[2:-1]
            data = data.replace('\"', '')
            data = data.replace('\'', '')
            data = data.replace('%', '')
            data = data.strip()
        else:
            data = 'missing'
        return data  # data.encode()

    def run(self):
        # try:
        cnx = self.database_connect()
        cursor = cnx.cursor()

        # ideally the most recent -- or most text will be at the top
        #    sql = 'select name, url, id, body from articles order by url, LENGTH(body) desc'
        # sql = 'select name, url, id, body, source, author from articles'
        sql = 'select a.name, a.url, a.id, a.body, a.source, a.author  from articles a left outer join marks m on a.Id = m.article_id where m.article_id is null order by publish_date desc'

        cursor.execute(sql)
        articles = cursor.fetchall()
        cursor.close()
        X_train = []
        print(len(articles))
        print(f'Running language analytic on all articles')
        for article in tqdm(articles):
            cursor = cnx.cursor()
            name = self.clean_data(article[0])
            url = self.clean_data(article[1])
            id = article[2]
            body = self.clean_data(article[3])
            source = self.clean_data(article[4])
            author = self.clean_data(article[5])

            if len(body) >= 100000:
                body = body[0:99999]

            # detect language
            doc = nlp(body)

            language = doc._.language['language']
            score = doc._.language['score']
            try:
                sql = f'INSERT INTO marks (name, type, score, article_id) values ("{language}", "LID", "{score}", "{id}"  )'
                cursor.execute(sql)
                cnx.commit()
                cursor.close()
                # print(f'id:{id}, name: {name}, language: {doc._.language}')
            except Exception as e:
                print(e)

        # finally:
        cnx.close()


if __name__ == '__main__':
    analytic = LanguageDetection()
    analytic.run()
