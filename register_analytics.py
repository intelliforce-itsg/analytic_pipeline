import logging

from AnalyticPipeline.named_registries import NamedRegistries

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")

@NamedRegistries.Analytics.register
class LanguageDetection():

    def __init__(self):
        self.logger = logging.getLogger('initializing Language Detection analytic')
        self.analytic = LanguageDetection()

    def run(self):
        self.logger.info(f'running Language Detection')
        self.analytic.run()


@NamedRegistries.Analytics.register
class NER1():
    def __init__(self):
        self.logger = logging.getLogger('initializing NER1 analytic')

    def run(self):
        self.logger.info(f'running NER1')


@NamedRegistries.Analytics.register
class NER2():
    def __init__(self):
        self.logger = logging.getLogger('initializing NER1 analytic')

    def run(self):
        self.logger.info(f'running NER2')


@NamedRegistries.Analytics.register
class Topic1():
    def __init__(self):
        self.logger = logging.getLogger('initializing Topic1 analytic')

    def run(self):
        self.logger.info(f'running Topic1')

@NamedRegistries.Analytics.register
class Sentiment1():
    def __init__(self):
        self.logger = logging.getLogger('initializing Sentiment1 analytic')

    def run(self):
        self.logger.info(f'running Sentiment1 analytic')


class RegisterAnalytics():
    def __init__(self):
        self.logger = logging.getLogger('register analytics')
        self.logger.info(f'registering analytics')

