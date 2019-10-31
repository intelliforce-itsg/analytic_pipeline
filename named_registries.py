import logging
from named_registry import NamedRegistry

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")

logger = logging.getLogger('named registry')


class NamedRegistries():
    """A class containing named registry collections"""
    # This is static
    registries = {}

    Ingesters = NamedRegistry("Ingesters")
    Preprocessors = NamedRegistry("Preprocessors")
    Analytics = NamedRegistry("Analytics")
    Scorers = NamedRegistry("Scorers")
    Models = NamedRegistry("Models")
    Deployers = NamedRegistry("Deployers")
    Postprocessors = NamedRegistry("Postprocessors")

    # E.g ingest, preprocess, analytics, scoring, model deployment
    Pipelines = NamedRegistry("Pipelines")

    def __init__(self):
        raise RuntimeError("Registries is not intended to be instantiated.")
