#!/usr/bin/env python3
"""
Cytomine settings module. This file makes sure that the loaded settings file is
sound, and that all values are present.
"""

import logging
from argparse import Namespace
from typing import Dict

import yaml
from yaml.parser import ParserError

def parse_to_namespaces(data: Dict, target):
    """
    Recursively loads the `data` dictionary into attributes of `target` (ex.
    `{'core': {'url': "ex", 'protocol': "https"}}` becomes:
    `<target>.core.url = "ex"; <target>.core.protocol = "https"`).
    """
    assert isinstance(data, dict)
    for key, value in data.items():
        if hasattr(target, key):
            attribute = getattr(target, key)
            if isinstance(attribute, Namespace):
                parse_to_namespaces(value, attribute)
            else:
                setattr(target, key, value)


class Settings():

    def __init__(self, filename: str = "settings.yaml"):
        """
        Sets default values for all settings, lthen loads the given settings
        file by calling `load_yaml(filename)`.
        """
        self.core = Namespace(
            url="core",
            public_key="",
            private_key="",
            protocol="https"
        )
        self.rabbitmq = Namespace(
            host="rabbitmq",
            port=5672,
            username="",
            password="",
            queue="",
            exchange=""
        )
        self.github = Namespace(
            username="",
            password=""
        )
        self.serviceaccount = "cytomine"
        self.software_repos = []

        self.load_yaml(filename)

    def load_yaml(self, filename: str):
        """
        Loads all available settings values from the yaml file, `filename`.
        """
        assert isinstance(filename, str)

        logging.info("Reading settings file %s", filename)
        try:
            raw_settings = yaml.safe_load(open(filename).read())
        except FileNotFoundError:
            logging.error("Settings file not found")
            return
        except ParserError:
            logging.error("Settings file not a valid yaml file")
            return

        # load the settings by comparing the variables defined on self to those
        # in `raw_settings`
        parse_to_namespaces(raw_settings, self)



