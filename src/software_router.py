#!/usr/bin/env python3
"""
This is a Cytomine software router for use in kubernetes.
"""

import time
import json
import logging
from pika.credentials import PlainCredentials

import yaml

import pika
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
        core = cytomine.Cytomine.connect(settings['url'],
                                         settings['publicKey'],
                                         settings['privateKey'])
        if core.current_user:
            return core
    except JSONDecodeError:
        logging.error("Failed to connect to cytomine core")
    except yaml.parser.ParserError:
        logging.error("Failed to read settings yaml file")

def connect_to_rabbitmq(settings):
    """
    Connects to rabbitmq using the values in settings. Returns a `connection`.
    """
    logging.info("Connecting to rabbitmq")
    try:
        connection_params = pika.ConnectionParameters(
            host=settings['host'],
            port=settings['port'],
            credentials=pika.PlainCredentials(settings['username'],
                                              settings['password'])
        )
        return pika.BlockingConnection(connection_params)
    except pika.exceptions.AMQPConnectionError:
        logging.error("Couldn't connect to rabbitmq")

def queue_callback(ch, method, properties, body):
    """
    Callback function that handles rabbitmq messages for the software router.
    """
    msg = json.loads(body)
    logging.info("Received: %s", msg)

    ch.basic_ack(method.delivery_tag)

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
    rabbitmq = None
    channel = None

    # Main program loop.
    while True:
        try:

            if not settings:
                settings = load_settings_file(args.settings_file)
                # if the settings have changed we should reconnect to core and
                # rabbitmq
                core = None
                rabbitmq = None

            if settings and not core:
                core = connect_to_core(settings['core'])

            if settings and core and (not rabbitmq or not rabbitmq.is_open):
                rabbitmq = connect_to_rabbitmq(settings['rabbitmq'])
                # connect to the default channel

            if rabbitmq and not channel:
                channel = rabbitmq.channel()
                channel.basic_consume(queue=settings['rabbitmq']['queue'],
                                        on_message_callback=queue_callback)
                # make sure that the channel exists
                channel.queue_declare(settings['rabbitmq']['queue'],
                                      durable=True)
                channel.start_consuming()

        except pika.exceptions.ChannelClosedByBroker:
            channel = None
        # TODO: remove this catch-all exception once the program goes beyond
        # testing
        except Exception as e:
            logging.error("Uncaught exception! %s:%s", type(e), e)
        time.sleep(10)
