import logging

from named_registries import NamedRegistries
import os

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")

@NamedRegistries.Ingesters.register
class NewsAPiIngester():
    def __init__(self):
        os.join('production')
        # import production.ingest_newsapi
        # self.ingester = IngestNewsApi()
        self.logger = logging.getLogger('register news ingester')

    def run(self):
        self.logger.info(f'running news ingester')


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

