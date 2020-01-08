"""
Predict TOPIC

Usage: extract_topics.py [-h] [--text-file] [--ner-data-file=<input_file>] --model-file=<model_file>

Options:
  -h --help
  --text-file                     If the input file a raw test file
  --ner-data-file=<input_file>    Input file for training NER model.
  --model-file=<model_file>       Output model file [default: ../../models/ner-3.hdf5].
"""

import pickle

import matplotlib.pyplot as plt
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
from docopt import docopt
from keras.models import load_model
from keras.preprocessing.sequence import pad_sequences
from keras.utils import to_categorical
from future.utils import iteritems
from datetime import date, datetime, timedelta
import pickle
import mysql.connector
from sys import exit
import spacy
import os
plt.style.use("ggplot")


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
        data = data.replace('b\'', '')
        data = data.replace('\"', '')
        data = data.replace('\'', '')
        data = data.replace('%', '')
        data = data.strip()
    else:
        data = 'missing'
    return data

def vectorize_sequences(sequences, dimension):
    results = np.zeros((len(sequences), dimension))
    for i, sequence in enumerate(sequences):

        try:
            for word in sequence:
                results[i][word] = 1
        except Exception as e:
            print(e)

    return results


def predict_document_topic(model, document):
    """

    :param document: list of sentences, sentence are lists of text words
    :return: a list of sentence predictions padded to max_sentence_length
    """

    # Parse the document into sentences
    doc = nlp(clean_data(document))
    document = []
    for sent in doc.sents:
        for word in sent:
            document.append(word.string.strip())

    # Convert the document to word encodings
    document_idx = []
    for word in document:
        if word in word2idx:
            document_idx.append(word2idx[word])
        else:
            document_idx.append(word2idx['UNKNOWN'])

    highest_word_idx = max(word2idx.values())
    #data = [[word2idx[w] for w in d] for d in documents]

    vectorized_document = vectorize_sequences([document_idx], highest_word_idx + 1)


    # Predict the TOPIC tags
    prediction = model.predict(vectorized_document)

    document_prediction = []
    # process each sentence
    # for prediction in predictions:
    # # for sentence_idx in range(len(predictions)):
    #     # predict each sentence
    #     # convert the predictions from OHE to integers
    #     document_tag_predictions.append(np.argmax(prediction, axis=-1))

    return document, vectorized_document, prediction


if __name__ == '__main__':

    arguments = docopt(__doc__)

    # defaults
    word2idx = None
    tag2idx = None
    topic_data_file = None
    model_file = '../models/topic-model-mlp-en-0.90.hdf5'
    other_model_file='../models/topic-model-mlp-en-0.90.hdf5'
    use_text_file = False

    language = 'en'
    row_limit = 100

    if arguments['--ner-data-file'] is not None:
        topic_data_file = arguments['--ner-data-file']
        print(f"Using input file {topic_data_file}")

    if arguments['--model-file'] is not None:
        model_file = arguments['--model-file']
        print(f"Using model file {model_file}")

    if arguments['--text-file'] == True:
        use_text_file = True
        print(f"Input is a raw text file")

    model = load_model(model_file)
    print(f'loading model file: {model_file}')

    # word dictionary
    with open(f'../models/topic.word2idx.pickle', 'rb') as handle:
        word2idx = pickle.load(handle)
        idx2word = {v: k for k, v in iteritems(word2idx)}

    # label_encoder
    with open(f'../models/topic.tag2idx.pickle', 'rb') as handle:
        tag2idx = pickle.load(handle)
    idx2tag = {v: k for k, v in iteritems(tag2idx)}

    # process text data in english
    nlp = spacy.load('en')
    documents = []
    all_sentences = []
    all_words = []
    if use_text_file is False:
        cnx = database_connect()
        cursor = cnx.cursor()

        # sql = f"select articles.name, articles.url, articles.body, tt.tag from articles inner join marks m on articles.Id = m.article_id inner join topic_tags tt on articles.Id = tt.article_id where m.name = '{language}' and tt.tag = '{tag}' order by publish_date desc LIMIT {row_limit}"
        sql = f"select articles.name, articles.url, articles.body, tt.tag, articles.id from articles inner join marks m on articles.Id = m.article_id inner join topic_tags tt on articles.Id = tt.article_id where m.name = '{language}' order by publish_date desc LIMIT {row_limit}"

        cursor.execute(sql)
        articles = cursor.fetchall()
        X_train = []
        print(len(articles))
        for x in articles:
            X_train.append([clean_data(x[0]), clean_data(x[1]), clean_data(x[2]), clean_data(x[3]), x[4]])

        df = pd.DataFrame(X_train)
        df.columns = ['title', 'url', 'article', 'tag', 'id']
        print(f'Predicting articles: {len(X_train)}')

        endpad_idx = word2idx['ENDPAD']

        for index, article in df.iterrows():

            title = article['title']
            article_id = article['id']
            body = article['article']
            tag = article['tag']
            document, vectorized_document, prediction = predict_document_topic(model, body)
            prediction_idx = np.argmax(prediction)
            predicted_tag = idx2tag[prediction_idx]
            #if tag == 'FED_AI' or predicted_tag == 'FED_AI':
            print(f'TITLE: {title}')
            print(f'  tag:{tag}, predicted:{predicted_tag}')

            # for sentence_idx, sentence_tag_predictions in enumerate(document_tag_predictions):
            #     for word_idx, word_tag_prediction in enumerate(sentence_tag_predictions):
            #         if endpad_idx != padded_document[sentence_idx][word_idx]:
            #             word = idx2word[padded_document[sentence_idx][word_idx]]
            #             if word == 'UNKNOWN':
            #                 try:
            #                     word = document[sentence_idx][word_idx]
            #                 except Exception as e:
            #                     pass
            #             predicted_tag = idx2tag[word_tag_prediction]
            #             if predicted_tag != 'O':
            #                 add_named_entity_sql = f'INSERT into named_entities (named_entity, word, article_id, sentence_index, word_index) VALUES ("{predicted_tag}", "{word}", {article_id}, {sentence_idx}, {word_idx} )'
            #                 cursor.execute(add_named_entity_sql)


                        #print(f'{word}:{predicted_tag}')

            #print('')

            #cnx.commit()

