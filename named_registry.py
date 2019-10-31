import logging

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG,
                    datefmt="%m/%d/%Y %H:%M:%S")

logger = logging.getLogger('named registry')


class NamedRegistry():
    """A class containing a single named registry"""

    def __init__(self, registry_name):
        # these are not static
        self.registry_name = registry_name
        self.registrations = {}

    def register(self, thing_to_register):
        self.registrations[thing_to_register.__name__] = thing_to_register
        logger.info(f"Added to registry: {self.registry_name} {thing_to_register.__name__}")
