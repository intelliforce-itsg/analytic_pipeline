import logging

from AnalyticPipeline.named_registries import NamedRegistries

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")

@NamedRegistries.Pipelines.register
class NewsPipeline():

    def __init__(self):
        self.logger = logging.getLogger('register News pipeline')

    def build_pipeline(self):
        self.pipeline = []

        # INGEST
        self.pipeline.append(NamedRegistries.Ingesters.registrations['NewsAPiIngester']())

        # PREPROCESS
        self.pipeline.append(NamedRegistries.Preprocessors.registrations['DeduplicateDatabaseRecordsPreprocessor']())
        self.pipeline.append(NamedRegistries.Preprocessors.registrations['CleanDatabaseArticlesPreprocessor']())

        # ANALYTICS
        self.pipeline.append(NamedRegistries.Analytics.registrations['LanguageDetection']())
        self.pipeline.append(NamedRegistries.Analytics.registrations['Topic1']())
        self.pipeline.append(NamedRegistries.Analytics.registrations['NER1']())
        self.pipeline.append(NamedRegistries.Analytics.registrations['Sentiment1']())

        #POSTPROCESS
        self.pipeline.append(NamedRegistries.Postprocessors.registrations['NewsPostprocessor']())

    def run(self):
        self.logger.info(f'building news pipeline')
        self.build_pipeline()

        logging.info('running news pipeline')
        for analytic in self.pipeline:
            self.logger.debug(f'Start')
            analytic.run()
            self.logger.debug(f'End')


    def run(self):
        self.logger.info(f'building twitter pipeline')
        self.build_pipeline()

        logging.info('running news pipeline')
        for analytic in self.pipeline:
            self.logger.debug(f'Start')
            analytic.run()
            self.logger.debug(f'End')


class RegisterPipelines():
    def __init__(self):
        self.logger = logging.getLogger('register pipelines')
        self.logger.info(f'registering pipelines')
