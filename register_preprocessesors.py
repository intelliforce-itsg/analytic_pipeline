import logging

from AnalyticPipeline.named_registries import NamedRegistries
from AnalyticPipeline.production.clean_database_articles import CleanDatabaseArticles
from AnalyticPipeline.production.dedup_database_records import DeduplicateDatabaseRecords

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")


@NamedRegistries.Preprocessors.register
class DeduplicateDatabaseRecordsPreprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register deduplicate database records preprocessor')
        self.preprocessor = DeduplicateDatabaseRecords()

    def run(self):
        self.logger.info(f'running deduplicate database records preprocessor')
        self.preprocessor.run()


@NamedRegistries.Preprocessors.register
class DeduplicateDatabaseRecordsPreprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register clean database articles preprocessor')
        self.preprocessor = CleanDatabaseArticles()

    def run(self):
        self.logger.info(f'running  clean database articles preprocessor')
        self.preprocessor.run()



@NamedRegistries.Preprocessors.register
class CSVPreprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register csv preprocessor')

    def run(self):
        self.logger.info(f'running csv preprocessor')


@NamedRegistries.Preprocessors.register
class RawTextPreprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register raw text preprocessor')

    def run(self):
        self.logger.info(f'running raw text preprocessor')


@NamedRegistries.Preprocessors.register
class DatabaseTextPreprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register database preprocessor')

    def run(self):
        self.logger.info(f'running database text preprocessor')


@NamedRegistries.Preprocessors.register
class TweetPreprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register tweet preprocessor')

    def run(self):
        self.logger.info(f'running tweet preprocessor')


class RegisterPreprocessors():
    def __init__(self):
        self.logger = logging.getLogger('register preprocessors')
        self.logger.info(f'registering preprocessors')
