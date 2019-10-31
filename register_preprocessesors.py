import logging

from named_registries import NamedRegistries

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")


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
