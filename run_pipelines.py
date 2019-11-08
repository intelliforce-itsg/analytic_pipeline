import logging
import time

import schedule
import threading
import concurrent.futures
from tqdm import tqdm

from AnalyticPipeline.register_ingesters import RegisterIngesters
from AnalyticPipeline.register_preprocessesors import RegisterPreprocessors
from AnalyticPipeline.register_analytics import RegisterAnalytics
from AnalyticPipeline.register_postprocessors import RegisterPostprocessors
from AnalyticPipeline.register_pipelines import RegisterPipelines
from AnalyticPipeline.named_registries import NamedRegistries

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")

class RunPipelines():
    def __init__(self):
        self.logger = logging.getLogger('run pipelines')

        # These keep the imports from being optimized away
        RegisterIngesters()
        RegisterPreprocessors()
        RegisterAnalytics()
        RegisterPostprocessors()
        RegisterPipelines()

    def job_run_pipelines(self):
        # This would be triggered by a scheduler, timer, a rest call, or queue etc
        self.logger.info(f'Running analytic pipelines')
        pipelines_to_run = len(NamedRegistries.Pipelines.registrations)

        for name, pipeline in NamedRegistries.Pipelines.registrations.items():
            self.logger.info(f'**************************************************************************************')
            self.logger.info(f'Pipeline: {name}')

            # Launch of each pipeline
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                executor.map(pipeline().run(), range(1))

if __name__ == "__main__":

    pipeline_runner = RunPipelines()

    # Thinking YAML injection

    schedule.every(12).hours.do(pipeline_runner.job_run_pipelines)
    # schedule.every().hour.do(job)
    # schedule.every().day.at("10:30").do(job)
    # schedule.every(5).to(10).minutes.do(job)
    # schedule.every().monday.do(job)
    # schedule.every().wednesday.at("13:15").do(job)
    # schedule.every().minute.at(":17").do(job)

    while True:
        schedule.run_pending()
        time.sleep(10)

    # for name, analytic in NamedRegistries.Analytics.registrations.items():
    #     logging.(f'Analytic: {name}')
    #     analytic().run()
    #
    # # This would be triggered by a scheduler, timer, a rest call, or queue etc
    # for name, pipeline in NamedRegistries.Pipelines.registrations.items():
    #     logging.(f'Pipeline: {name}')
    #     pipeline().run()
