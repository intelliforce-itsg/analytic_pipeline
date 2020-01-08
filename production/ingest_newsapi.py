import os
# from __future__ import print_function
from datetime import datetime as dt
from datetime import timedelta

from sys import exit

import mysql.connector
import pandas as pd
import requests
from bs4 import BeautifulSoup
# Retrieve set environment variables
from tqdm import tqdm
import spacy
spacy.prefer_gpu()

PASSWORD = os.environ.get('NEWS_DB_PASSWORD')

query_topic_dict = {}
query_topic_dict['Federal AI'] = 'FED_AI'
query_topic_dict['artificial intelligence'] = 'AI'
query_topic_dict['ai'] = 'AI'
query_topic_dict['machine learning'] = 'ML'
query_topic_dict['technology'] = 'TECHNOLOGY'
query_topic_dict['money'] = 'MONEY'
query_topic_dict['economics'] = 'ECONOMICS'
query_topic_dict['finance'] = 'FINANCE'
query_topic_dict['travel'] = 'TRAVEL'
query_topic_dict['destination'] = 'TRAVEL'
query_topic_dict['politics'] = 'POLITICS'
query_topic_dict['manufacturing'] = 'MANUFACTURING'
query_topic_dict['Additive Manufacturing'] = 'ADDITIVE_MANUFACTURING'
query_topic_dict['robotics'] = 'ROBOTICS'
query_topic_dict['nasa'] = 'NASA'
query_topic_dict['dod'] = 'DOD'
query_topic_dict['NSA'] = 'NSA'
query_topic_dict['\"ai+initiative\"'] = 'FED_AI'
query_topic_dict['\"ai+executive+order\"'] = 'FED_AI'
query_topic_dict['JAIC'] = 'FED_AI'
query_topic_dict['Military AI Complex'] = 'FED_AI'
query_topic_dict['war'] = 'WAR'
query_topic_dict['terror'] = 'TERROR'
query_topic_dict['terrorist'] = 'TERROR'
query_topic_dict['military'] = 'MILITARY'
query_topic_dict['medical'] = 'MEDICAL'
query_topic_dict['pharmaceutical'] = 'MEDICAL'
query_topic_dict['medicine'] = 'MEDICAL'
query_topic_dict['pharmacy'] = 'MEDICAL'
query_topic_dict['surgical'] = 'MEDICAL'
query_topic_dict['american ai'] = 'AI'
query_topic_dict['cyber'] = 'CYBER'
query_topic_dict['cybercom'] = 'CYBER'
query_topic_dict['exploit'] = 'CYBER'
query_topic_dict['zero day'] = 'CYBER'
query_topic_dict['federal'] = 'FEDERAL'
query_topic_dict['government'] = 'GOVERNMENT'


class IngestNewsApi():

    def __init__(self):
        self.PASSWORD = os.environ.get('NEWS_DB_PASSWORD')
        self.attempts = 3

    def NewsApiV2(self, query, from_date, to_date):
        """
            articles": [
            -{
            -"source": {
            "id": "the-new-york-times",
            "name": "The New York Times"
            },
            "author": "Matina Stevis-Gridneff",
            "title": "E.U.’s New Digital Czar: ‘Most Powerful Regulator of Big Tech on the Planet’",
            "description": "Margrethe Vestager, whose billion-dollar fines have made her loathed by Silicon Valley, has won new powers that give her unrivaled regulatory reach. President Trump says she “hates the United States.”",
            "url": "https://www.nytimes.com/2019/09/10/world/europe/margrethe-vestager-european-union-tech-regulation.html",
            "urlToImage": "https://static01.nyt.com/images/2019/09/10/world/10vestager-1/10vestager-1-facebookJumbo.jpg",
            "publishedAt": "2019-09-10T21:39:45Z",
            "content": "The unique characteristics of the digital world, including the inability for both consumers and regulators to see the algorithms that determine what users see in search results and news feeds, and what advertisers pay to reach them,make righting any potential… [+1513 chars]"
            },
        """
        articles = []
        api_key = '350731e43e80441594481508c92c4aa9'

        news_url = f'https://newsapi.org/v2/everything?q={query}&from={from_date}&to={to_date}&sortBy=popularity&pageSize=100&page=1&apiKey={api_key}'
        news = requests.get(news_url).json()
        total_results = int(news["totalResults"])

        pages = int((total_results / 100) + 1)
        print(f'querying {query} from {from_date} to {to_date}')
        for page in tqdm(range(1, pages + 1, 1)):
            attempts_remaining = self.attempts
            complete = False
            while attempts_remaining > 0 and not complete:
                news_url = f'https://newsapi.org/v2/everything?q={query}&from={from_date}&to={to_date}&sortBy=popularity&pageSize=100&page={page}&apiKey={api_key}'
                try:
                    news = requests.get(news_url).json()
                    articles.extend(news["articles"])
                    complete = True
                except:
                    attempts_remaining -= 1
                    print(f'query attempts_remaining {attempts_remaining} for {news_url}')

        return articles

    def NewsApiV2Everything(self, query):
        """
            articles": [
            -{
            -"source": {
            "id": "the-new-york-times",
            "name": "The New York Times"
            },
            "author": "Matina Stevis-Gridneff",
            "title": "E.U.’s New Digital Czar: ‘Most Powerful Regulator of Big Tech on the Planet’",
            "description": "Margrethe Vestager, whose billion-dollar fines have made her loathed by Silicon Valley, has won new powers that give her unrivaled regulatory reach. President Trump says she “hates the United States.”",
            "url": "https://www.nytimes.com/2019/09/10/world/europe/margrethe-vestager-european-union-tech-regulation.html",
            "urlToImage": "https://static01.nyt.com/images/2019/09/10/world/10vestager-1/10vestager-1-facebookJumbo.jpg",
            "publishedAt": "2019-09-10T21:39:45Z",
            "content": "The unique characteristics of the digital world, including the inability for both consumers and regulators to see the algorithms that determine what users see in search results and news feeds, and what advertisers pay to reach them,make righting any potential… [+1513 chars]"
            },
        """
        articles = []

        api_key = '350731e43e80441594481508c92c4aa9'

        news_url = f'https://newsapi.org/v2/everything?q={query}&sortBy=popularity&pageSize=100&page=1&apiKey={api_key}'
        news = requests.get(news_url).json()
        total_results = int(news["totalResults"])
        pages = int((total_results / 100) + 1)

        print(f'querying {query} everything')
        for page in tqdm(range(1, pages + 1, 1)):
            attempts_remaining = self.attempts
            complete = False
            while attempts_remaining > 0 and not complete:
                news_url = f'https://newsapi.org/v2/everything?q={query}&pageSize=100&page{page}&apiKey={api_key}'
                try:
                    news = requests.get(news_url).json()
                    articles.extend(news["articles"])
                    complete = True
                except:
                    attempts_remaining -= 1
                    print(f'query attempts_remaining {attempts_remaining} for {news_url}')

        return articles

    def UsaTodaySource(self):

        # for news_source in news_sources:

        api_key = '350731e43e80441594481508c92c4aa9'

        news_url = f'https://newsapi.org/v2/everything?sources=usa-today&pageSize=100&page=1&apiKey={api_key}'
        news = requests.get(news_url).json()
        total_results = int(news["totalResults"])
        pages = int((total_results / 100) + 1)

        for page in tqdm(range(1, pages + 1, 1)):
            news_url = f'https://newsapi.org/v2/everything?sources=usa-today&pageSize=100&page={page}&apiKey={api_key}'
            # news_url =f'https://newsapi.org/v2/top-headlines?sources=usa-today&apiKey={api_key}'
            # news_url = f'https://newsapi.org/v1/articles?source=usa-today&sortBy=top&apiKey={api_key}'

            # Get the headlines and urls
            news = requests.get(news_url).json()
            top_articles = news["articles"]

            data = pd.DataFrame.from_dict(top_articles)

            article_bodies = {}
            for url in data['url']:
                news = requests.get(url)
                news_soup = BeautifulSoup(news.content)
                slugs = news_soup.findAll("p", {'class': 'p-text'})
                text = ''
                for slug in slugs:
                    text += ' ' + slug.text
                article_bodies[url] = text

            data['body'] = article_bodies.values()

        return data

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
            if data.startswith("b\'") or data.startswith("b'"):
                data = data[2:-1]
            data = data.replace('\"', '')
            data = data.replace('\'', '')
            data = data.replace('%', '')
            data = data.strip('[]')
            data = data.strip()
        else:
            data = 'missing'
        return data  # data.encode()

    def is_duplicate(self, cursor, name, url):
        duplicate = False
        try:
            sql = f'SELECT id, name, url, body, publish_date, author, source from articles where url = "{self.clean_data(url)}" and name = "{name}"'
            cursor.execute(sql)
            articles = cursor.fetchall()
            if (len(articles) > 0):
                duplicate = True
            for article in articles:
                body = article[3]
                if 'chars]' in body:
                    duplicate = False
        except Exception as e:
            print(e)
            try:
                sql = f'SELECT id, name, url, body, publish_date, author, source from articles where url = "{self.clean_data(url)}" and name = "{name}"'
                cursor.execute(sql)
                articles = cursor.fetchall()
                if (len(articles) > 0):
                    duplicate = True
                for article in articles:
                    body = article[3]
                    if 'chars]' in body:
                        duplicate = False
            except Exception as e:
                pass
        return duplicate

    def ingest_newsapi_v2_source(self, cnx, cursor, query, topic_tag, from_date, to_date):
        # Collect the news
        articles = self.NewsApiV2(query, from_date, to_date)
        # articles = self.NewsApiV2Everything(query)

        print(f'Adding {len(articles)} for topic {topic_tag} from {from_date} to {to_date}')
        for article in tqdm(articles):
            author = article["author"]
            title = article["title"]
            content = article["content"]
            url = article["url"]
            image_url = article["urlToImage"]
            publish_date = article["publishedAt"]
            source = article["source"]["name"]
            # None or url

            title = self.clean_data(title)
            author = self.clean_data(author)
            content = self.clean_data(content)
            source = self.clean_data(source)
            publish_date_obj = dt.strptime(publish_date, "%Y-%m-%dT%H:%M:%SZ")
            url = self.clean_data(url)
            image_url = self.clean_data(image_url)

            duplicate = self.is_duplicate(cursor, title, url)

            if duplicate is False:
                try:
                    add_artcle = f'INSERT INTO articles(name, url, image_url, body, source, author, publish_date ) VALUES ("{title}", "{url}", "{image_url}", "{content}", "{source}", "{author}", "{publish_date_obj}" )'
                    cursor.execute(add_artcle)

                    article_id = cursor.lastrowid

                    add_topic_tag = f'INSERT INTO topic_tags(tag, article_id) VALUES ("{topic_tag}", "{article_id}")'
                    cursor.execute(add_topic_tag)

                    cnx.commit()
                except Exception as e:
                    cnx.rollback()
        return

    def ingest_newsapi_v2_source_everything(self, cnx, cursor, query, topic_tag):
        # Collect the news
        # articles = NewsApiV2(query, from_date, to_date)
        articles = self.NewsApiV2Everything(query)

        print(f'Adding {len(articles)} for topic {topic_tag} everything')
        for article in tqdm(articles):
            author = article["author"]
            title = article["title"]
            content = article["content"]
            url = article["url"]
            publish_date = article["publishedAt"]
            source = article["source"]["name"]

            title = self.clean_data(title)
            author = self.clean_data(author)
            content = self.clean_data(content)
            source = self.clean_data(source)
            publish_date_obj = dt.strptime(publish_date, "%Y-%m-%dT%H:%M:%SZ")

            duplicate = self.is_duplicate(cursor, title, url)

            if duplicate is False:
                try:
                    add_artcle = f'INSERT INTO articles(name, url, body, source, author, publish_date ) VALUES ("{title}", "{url}", "{content}", "{source}", "{author}", "{publish_date_obj}" )'
                    cursor.execute(add_artcle)

                    article_id = cursor.lastrowid

                    add_topic_tag = f'INSERT INTO topic_tags(tag, article_id) VALUES ("{topic_tag}", "{article_id}")'
                    cursor.execute(add_topic_tag)

                    cnx.commit()
                except:
                    cnx.rollback()

    def run(self):
        default_tag = 'TBD'
        error_tag = 'ERROR'

        try:
            # Connect to the Database
            cnx = self.database_connect()

            # Open a database cursor
            cursor = cnx.cursor()


            now = dt.now()
            current_date = now.strftime("%Y-%m-%d")

            # Grab all federal ai
            # ingest_newsapi_v2_source_everything(cnx, cursor, 'Federal AI', 'FED_AI')

            # for days_to_subtract in range(30):

            #  How many days in the past are we querying?
            days_to_subtract = 1
            from_date = (now - timedelta(days=days_to_subtract)).strftime("%Y-%m-%d")
            to_date = (now).strftime("%Y-%m-%d")

            for query in query_topic_dict.keys():
                tag = query_topic_dict[query]
                self.ingest_newsapi_v2_source(cnx, cursor, query, tag, from_date, to_date)
        finally:
            cursor.close()
            cnx.close()


if __name__ == '__main__':
    ingestor = IngestNewsApi()
    ingestor.run()
