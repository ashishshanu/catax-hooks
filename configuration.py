import configparser
import os


class Configuration:
    """ Get config from config file or environment variables.
    The priority order of config is:  Environment > Config file.
    The env var is composed by: [SECTION]_[OPTION]
    For example:
    get('smtp', 'bind_address', '127.0.0.1')
    => os.environ.get('smtp_bind_address', '127.0.0.1')
    """

    def __init__(self):
        pass

    config = configparser.ConfigParser()

    @staticmethod
    def init_config(file):
        if file:
            Configuration.config.read(file)

    @staticmethod
    def get(section, config):
        env_config = os.environ.get('_'.join([section, config]).upper())
        if env_config:
            return env_config
        else:
            return Configuration.config.get(section, config)
