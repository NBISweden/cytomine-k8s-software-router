#!/usr/bin/env python3
"""
This is a Cytomine software router for use in kubernetes.
"""

import time
import logging

import yaml

import cytomine

from json.decoder import JSONDecodeError


def load_settings_file(filename):
    """
    Returns a settings dictionary from the data in `filename` or None
    """
    logging.info("Reading settings file %s", filename)
    try:
        return yaml.safe_load(open(filename).read())
    except FileNotFoundError:
        logging.error("Settings file not found")
        return None

def connect_to_core(settings):
    """
    Connects to cytomine core and returns a cytomine object, or None if the
    connection failed.
    """
    logging.info("Connecting to cytomine core")
    try:
        return cytomine.Cytomine.connect(settings['url'],
                                         settings['publicKey'],
                                         settings['privateKey'])
    except JSONDecodeError:
        logging.error("Failed to connect to cytomine core")
    except yaml.parser.ParserError:
        logging.error("Failed to read settings yaml file")

if __name__ ==  "__main__":

    import argparse

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--settings-file", help="Settings file to read",
                        default="settings.yaml")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    logging.info("Starting")

    # initialize variables
    core = None
    settings = None

    # Main program loop.
    while True:

        if not settings:
            settings = load_settings_file(args.settings_file)
            # if the settings have changed we should reconnect to core
            core = None

        if settings and not core:
            core = connect_to_core(settings['core'])


        time.sleep(10)
