import configparser
import os

def get_config(section, key):
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.conf')
    config = configparser.ConfigParser()
    config.read(os.path.abspath(config_path))
    return config[section][key]
