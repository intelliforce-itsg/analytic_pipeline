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
        clean_data = data.strip()
        clean_data = clean_data.replace("xc3x83xc2xa2xc3x82xc2x80xc3x82xc2x99", "'")
        clean_data = clean_data.replace("xe2x80x99", "'")
        clean_data = clean_data.replace('"', '\"')
        if clean_data.startswith("b\'") or  clean_data.startswith("b'"):
            clean_data = clean_data[2:-1]
        clean_data = clean_data.replace('\"', '')
        clean_data = clean_data.replace('\'', '')
        clean_data = clean_data.replace('%', '')
        clean_data = clean_data.strip()
    else:
        clean_data = 'missing'
    return clean_data


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
for article in tqdm(articles):
    name  = clean_data(article[0])
    url = clean_data(article[1])
    id = article[2]
    body = clean_data(article[3])
    source = clean_data(article[4])
    author = clean_data(article[5])
    update = f'update articles set name = "{name}", url = "{url}", body = "{body}", source = "{source}", author = "{author}" where ID = {id} '
    try:
        cursor = cnx.cursor()
        cursor.execute(update)
        cnx.commit()
        cursor.close()

    except Exception as e:
        print(f'article:{id}, name:{name}')
        print(e)
    finally:
        cursor.close()


#finally:
cnx.close()

from sys import exit
exit(0)