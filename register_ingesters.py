import logging

from AnalyticPipeline.named_registries import NamedRegistries
import os

from AnalyticPipeline.production.ingest_newsapi import IngestNewsApi

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")

@NamedRegistries.Ingesters.register
class NewsAPiIngester():
    def __init__(self):
        self.ingester = IngestNewsApi()
        self.logger = logging.getLogger('register news ingester')

    def run(self):
        self.logger.info(f'running news ingester')
        self.ingester.run()


@NamedRegistries.Ingesters.register
class TwitterIngester():
    def __init__(self):
        self.logger = logging.getLogger('production/ingest')

    def run(self):
        self.logger.info(f'running twitter ingester')

class RegisterIngesters():
    def __init__(self):
        self.logger = logging.getLogger('register ingesters')
        self.logger.info(f'registering ingesters')

