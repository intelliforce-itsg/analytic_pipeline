"""
Predict NER

Usage: extract_and_save_entities.py [-h] [--text-file] [--ner-data-file=<input_file>] --model-file=<model_file>

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
import mysql.connector
from sys import exit
import spacy
import os

from tqdm import tqdm

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


def predict_document_ner(document, max_sentence_length):
    """

    :param document: list of sentences, sentence are lists of text words
    :return: a list of sentence predictions padded to max_sentence_length
    """

    # Parse the document into sentences
    doc = nlp(clean_data(document))
    document = []
    for sent in doc.sents:
        sentence = []
        for word in sent:
            sentence.append(word.string.strip())
        document.append(sentence)

    # Convert the document to word encodings
    document_idx = []
    for sentence in document:
        sentence_idx = []
        for word in sentence:
            if word in word2idx:
                sentence_idx.append(word2idx[word])
            else:
                sentence_idx.append(word2idx['UNKNOWN'])
        document_idx.append(sentence_idx)

    # Pad each sentence to max_sentence_length using the ENDPAD integer
    padded_document = pad_sequences(maxlen=max_sentence_length, sequences=document_idx, padding="post", value=word2idx['ENDPAD'])

    # Predict the NER tags
    predictions = model.predict(np.array(padded_document))

    document_tag_predictions = []
    # process each sentence
    for prediction in predictions:
    # for sentence_idx in range(len(predictions)):
        # predict each sentence
        # convert the predictions from OHE to integers
        document_tag_predictions.append(np.argmax(prediction, axis=-1))

    return document, padded_document, document_tag_predictions


if __name__ == '__main__':

    arguments = docopt(__doc__)

    try:
        # defaults
        word2idx = None
        tag2idx = None
        ner_data_file = None
        model_file = '../../models/ner-bilstm.hdf5'
        other_model_file='../../models/ner-bilstm.hdf5'
        use_text_file = False

        if arguments['--ner-data-file'] is not None:
            ner_data_file = arguments['--ner-data-file']
            print(f"Using input file {ner_data_file}")

        if arguments['--model-file'] is not None:
            model_file = arguments['--model-file']
            print(f"Using model file {model_file}")

        if arguments['--text-file'] == True:
            use_text_file = True
            print(f"Input is a raw text file")

        model = load_model(model_file)
        print(f'loading model file: {model_file}')

        # word dictionary
        with open(f'../../models/ner.word2idx.pickle', 'rb') as handle:
            word2idx = pickle.load(handle)
            idx2word = {v: k for k, v in iteritems(word2idx)}

        # label_encoder
        with open(f'../../models/ner.tag2idx.pickle', 'rb') as handle:
            tag2idx = pickle.load(handle)
        idx2tag = {v: k for k, v in iteritems(tag2idx)}


        # maxlen = max([len(s) for s in all_sentences])
        # print(f'Total Sentences: {len(all_sentences)}')
        # print(f'max sentence length from data {maxlen} setting to 256')
        max_sentence_length = 256


        # process text data in english
        nlp = spacy.load('en')
        documents = []
        all_sentences = []
        all_words = []
        if use_text_file is False:
            cnx = database_connect()
            cursor = cnx.cursor()

            sql = 'select *  from articles inner join topic_tags tt on articles.ID = tt.article_id'

            cursor.execute(sql)
            articles = cursor.fetchall()
            X_train = []
            print(len(articles))
            for x in tqdm(articles):
                X_train.append([clean_data(x[0]), clean_data(x[1]), clean_data(x[2]), clean_data(x[8]), x[9]])

            df = pd.DataFrame(X_train)
            df.columns = ['title', 'url', 'article', 'tag', 'id']
            print(f'Training data articles: {len(X_train)}')

            endpad_idx = word2idx['ENDPAD']

            for index, article in df.iterrows():


                title = article['title']
                article_id = article['id']
                print(f'TITLE: {title}')
                document, padded_document, document_tag_predictions = predict_document_ner(article['article'], max_sentence_length)

                # Blow out the old named entities -- this is a really bad idea TODO fixme
                remove_named_entities_sql = f'delete from named_entities where article_id = {article_id}'
                cursor.execute(remove_named_entities_sql)

                for sentence_idx, sentence_tag_predictions in enumerate(document_tag_predictions):
                    for word_idx, word_tag_prediction in enumerate(sentence_tag_predictions):
                        if endpad_idx != padded_document[sentence_idx][word_idx]:
                            word = idx2word[padded_document[sentence_idx][word_idx]]
                            if word == 'UNKNOWN':
                                try:
                                    word = document[sentence_idx][word_idx]
                                except Exception as e:
                                    pass
                            predicted_tag = idx2tag[word_tag_prediction]
                            if predicted_tag != 'O':
                                add_named_entity_sql = f'INSERT into named_entities (named_entity, word, article_id, sentence_index, word_index) VALUES ("{predicted_tag}", "{word}", {article_id}, {sentence_idx}, {word_idx} )'
                                cursor.execute(add_named_entity_sql)


                            #print(f'{word}:{predicted_tag}')

                #print('')
                cnx.commit()
            cursor.close()
            cnx.close()
    finally:
        exit(0)













































