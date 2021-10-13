#!/usr/bin/env python3
"""
This is a Cytomine software router for use in kubernetes.
"""

import time
import json
import logging
from types import FunctionType

import yaml

import pika
import cytomine

from json.decoder import JSONDecodeError

class SoftwareRouter():
    """
    Cytomine Software router implementation for kubernetes
    """

    def __init__(self, settings):
        """
        Reads a yaml settings file `settings`.
        """
        self.settings = self._load_settings(settings)

        self.core = None
        self.rabbitmq = None

        self.channel = None

    def _create_channel(self, connection):
        """
        Creates the rabbitmq communication channel
        """
        self.channel = self.rabbitmq.channel(
            on_open_callback=self.add_default_queue
        )

    def _load_settings(self, filename):
        """
        Returns a settings dictionary from the data in `filename` or None
        """
        logging.info("Reading settings file %s", filename)
        try:
            return yaml.safe_load(open(filename).read())
        except FileNotFoundError:
            logging.error("Settings file not found")
            return None

    def add_queue(self, queue: str, callback: FunctionType, durable=True):
        """
        Adds a rabbitmq queue, consuming messages from `queue` with `callback`.
        """
        # make sure that the channel exists
        self.channel.queue_declare(queue, durable=durable)
        # add a message callback to the channel
        self.channel.basic_consume(queue=queue, on_message_callback=callback)

    def add_default_queue(self, channel, ):
        """
        Adds the default rabbitmq queue as `settings.rabbitmq.queue`.
        """
        self.add_queue(self.settings['rabbitmq']['queue'],
                       self.queue_callback)

    def connect_to_core(self):
        """
        Connects to cytomine core and sets `self.core` to a cytomine object, or
        `None` if the connection failed.
        """
        logging.info("Connecting to cytomine core")
        try:
            core = cytomine.Cytomine(
                host=self.settings['core']['url'],
                public_key=self.settings['core']['publicKey'],
                private_key=self.settings['core']['privateKey'],
                logging_handlers=logging.getLoggerClass().root.handlers
            )
            if core.current_user:
                self.core = core
                return
        except JSONDecodeError:
            logging.error("Failed to connect to cytomine core")
        except yaml.parser.ParserError:
            logging.error("Failed to read settings yaml file")
        self.core = None

    def connect_to_rabbitmq(self):
        """
        Connects to rabbitmq using the values in settings. Returns a `connection`.
        """
        logging.info("Connecting to rabbitmq")
        try:
            connection_params = pika.ConnectionParameters(
                host=self.settings['rabbitmq']['host'],
                port=self.settings['rabbitmq']['port'],
                credentials=pika.PlainCredentials(
                    self.settings['rabbitmq']['username'],
                    self.settings['rabbitmq']['password'])
            )
            self.rabbitmq = pika.SelectConnection(connection_params,
                on_open_callback=self._create_channel
            )
        except pika.exceptions.AMQPConnectionError:
            logging.error("Couldn't connect to rabbitmq")
            self.rabbitmq = None

    def on_job(self, ch, method, properties, body):
        """
        Handles cytomine job requests.
        """
        logging.info("Super success! %s", body)
        ch.basic_ack(method.delivery_tag)

    def queue_callback(self, ch, method, properties, body):
        """
        Callback function that handles rabbitmq messages for the software router.
        """
        msg = json.loads(body)
        logging.info("Received: %s", msg)
        if msg.get('requestType', None) == 'addProcessingServer':
            logging.info("new processing server: %s", msg['name'])
            self.add_queue(msg['name'], self.on_job)

        ch.basic_ack(method.delivery_tag)

    def start(self):
        """
        Starts the rabbitmq ioloop
        """
        self.rabbitmq.ioloop.start()

if __name__ ==  "__main__":

    import argparse

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--settings-file", help="Settings file to read",
                        default="settings.yaml")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    # Create software router object
    software_router = SoftwareRouter(args.settings_file)

    # Main loop. Mostly used to make sure that all connections are established
    while True:

        if not software_router.core:
            software_router.connect_to_core()

        # Don't connect to rabbitmq until there is a connection to core. This is
        # mainly because core tends to crash if the main queue is created before
        # it starts.
        if software_router.core and not software_router.rabbitmq:
            software_router.connect_to_rabbitmq()

            # Add main channel if the connection worked
            if software_router.rabbitmq:
                software_router.start()

        # sleep for a bit before checking again
        time.sleep(10)
