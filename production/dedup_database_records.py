import mysql.connector
import pandas as pd
import os
# Retrieve set environment variables
from tqdm import tqdm

class DeduplicateDatabaseRecords():
    def __init__(self):
        self.PASSWORD = os.environ.get('NEWS_DB_PASSWORD')

    def database_connect(self):
        cnx = mysql.connector.connect(
            host="localhost",
            database="news",
            user="whroe",
            passwd=self.PASSWORD
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
            if data.startswith("b\'") or  data.startswith("b'"):
                data = data[2:-1]
            data = data.replace('\"', '')
            data = data.replace('\'', '')
            data = data.replace('%', '')
            data = data.strip()
        else:
            data = 'missing'
        return data #data.encode()

    def run(self):
        try:
            cnx = self.database_connect()
            cursor = cnx.cursor()

            sql = 'select distinct(tag) from topic_tags'
            cursor.execute(sql)
            tags = cursor.fetchall()
            target_names = []
            for tag in tqdm(tags):
                target_names.append(self.clean_data(tag[0]))


            # ideally the most recent -- or most text will be at the top
            #sql = 'select name, url, id, body, publish_date from articles order by url, LENGTH(body) desc, publish_date desc'
            sql = 'select  a.name, a.url, a.id, a.body, a.publish_date, tt.tag from articles as a inner join topic_tags tt on a.ID = tt.article_id where tt.tag != "FED_AI" order by url, LENGTH(body) desc, publish_date desc'

            cursor.execute(sql)
            articles = cursor.fetchall()
            X_train = []
            print(f'Processing {len(articles)} not FED_AI articles')
            for x in tqdm(articles):
                X_train.append([self.clean_data(x[0]), self.clean_data(x[1]), x[2], self.clean_data(x[3]), x[4], self.clean_data(x[5])])

            df = pd.DataFrame(X_train)
            df.columns = ['name', 'url', 'id', 'body', 'publish_date', 'tag']
            cursor.close()

            cursor = cnx.cursor()
            last_url = ''
            for index, article in tqdm(df.iterrows()):

                url = article['url']
                tag = article['tag']
                body = article['body']
                name = article['name']
                id = article['id']


                # Skip FED_AI topics
                if url == last_url:
                    id = article['id']

                    print(f'Removing duplicated #{index}, ID: {id} TITLE: {name}')

                    # remove topic tags
                    remove_marks_sql = f'delete from marks where article_id = {id}'
                    cursor.execute(remove_marks_sql)

                    # remove topic tags
                    remove_topics_sql = f'delete from topic_tags where article_id = {id}'
                    cursor.execute(remove_topics_sql)

                    #remove named entities
                    remove_named_entities_sql = f'delete from named_entities where article_id = {id}'
                    cursor.execute(remove_named_entities_sql)

                    # remove record
                    dedup_sql = f'delete from articles where ID = {id}'
                    cursor.execute(dedup_sql)

                    cnx.commit()
                else:
                    last_url = url



            # ideally the most recent -- or most text will be at the top
            #sql = 'select name, url, id, body, publish_date from articles order by url, LENGTH(body) desc, publish_date desc'
            sql = 'select  a.name, a.url, a.id, a.body, a.publish_date, tt.tag from articles as a inner join topic_tags tt on a.ID = tt.article_id where tt.tag = "FED_AI" order by url, LENGTH(body) desc, publish_date desc'

            cursor.execute(sql)
            articles = cursor.fetchall()
            X_train = []
            print(f'Processing {len(articles)} FED_AI articles')
            for x in tqdm(articles):
                X_train.append([self.clean_data(x[0]), self.clean_data(x[1]), x[2], x[3], self.clean_data(x[5]) ])

            df = pd.DataFrame(X_train)
            df.columns = ['name', 'url', 'id', 'publish_date', 'tag']
            cursor.close()

            cursor = cnx.cursor()
            last_url = ''
            for index, article in tqdm(df.iterrows()):
                url = article['url']
                tag = article['tag']
                name = article['name']
                id = article['id']

                # Skip FED_AI topics
                if url == last_url:
                    print(f'Removing duplicated #{index}, ID: {id} TITLE: {name}')

                    id = article['id']
                    # remove topic tags
                    remove_marks_sql = f'delete from marks where article_id = {id}'
                    cursor.execute(remove_marks_sql)

                    # remove topic tags
                    remove_topics_sql = f'delete from topic_tags where article_id = {id}'
                    cursor.execute(remove_topics_sql)

                    #remove named entities
                    remove_named_entities_sql = f'delete from named_entities where article_id = {id}'
                    cursor.execute(remove_named_entities_sql)

                    # remove record
                    dedup_sql = f'delete from articles where ID = {id}'
                    cursor.execute(dedup_sql)

                    cnx.commit()
                else:
                    last_url = url


        finally:
            cursor.close()
            cnx.close()

if __name__ == '__main__':
    preprocessor = DeduplicateDatabaseRecords()
    preprocessor.run()
