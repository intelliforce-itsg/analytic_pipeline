import logging

from named_registries import NamedRegistries

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")


@NamedRegistries.Postprocessors.register
class NewsPostprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register csv postprocessor')

    def run(self):
        self.logger.info(f'running news postprocessor')


@NamedRegistries.Postprocessors.register
class TweetPostprocessor():
    def __init__(self):
        self.logger = logging.getLogger('register tweet postprocessor')

    def run(self):
        self.logger.info(f'running tweet postprocessor')


class RegisterPostprocessors():
    def __init__(self):
        self.logger = logging.getLogger('register postprocessors')
        self.logger.info(f'registering postprocessors')
