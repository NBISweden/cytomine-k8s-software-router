#!/usr/bin/env python3
"""
This is a Cytomine software router for use in kubernetes.
"""

import re
import os
import time
import json
import logging
import requests
import tempfile
from threading import Thread
from time import sleep
from types import FunctionType

import yaml

import pika
import cytomine
from cytomine.models import (
    ProcessingServerCollection,
    SoftwareCollection
)
from cytomine.models.software import Job, Software, SoftwareParameter
from cytomine.models.property import AttachedFile
from cytomine.models.user import CurrentUser
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from json.decoder import JSONDecodeError

from settings import Settings
from repository import Repository

def kube_label(name: str) -> str:
    """
    kubernetes job names must bu only lower case alphanumeric characters, or
    '-', and start and end on an alphanumeric character.
    """
    new_name = ""
    found_first = False
    for c in name.lower():
        if not found_first and c in '.-':
            continue
        found_first = True
        if re.match('[a-z0-9]', c):
            new_name += c
            continue
        c = '-'
        if new_name[-1] not in "-":
            new_name += c
    while new_name[-1] in '-':
        new_name = new_name[:-1]
    return new_name

def kube_job_label(job_msg) -> str:
    """
    Convenience function to crate a job label from a job message
    """
    return kube_label(f"{job_msg.name}-{job_msg.number}")

class SoftwareRouter():
    """
    Cytomine Software router implementation for kubernetes
    """

    def __init__(self, settings_file):
        """
        Reads a yaml settings file `settings_file`.
        """
        self.settings = Settings(settings_file)
        self.name = self.settings.name
        self.id = None

        self.core = None
        self.server = None
        self.rabbitmq = None
        self.repositories = {}
        for repo in self.settings.software_repos:
            self.add_repo(repo,
                          prefix="S_",
                          user=self.settings.github.username,
                          password=self.settings.github.password)

        # kubernetes job stuff
        self.kube_config = config.load_incluster_config()
        self.jobs_api = client.BatchV1Api()
        self.core_api = client.CoreV1Api()

        # Thread for checking if we're still connected to core
        self.core_ping_thread = None
        self.exiting = False
        self.start_ping_thread()

        self.channel = None

    def __del__(self):
        self.exiting = True
        if self.core_ping_thread:
            self.core_ping_thread.join()

    def _create_channel(self, connection):
        """
        Creates the rabbitmq communication channel
        """
        self.channel = self.rabbitmq.channel(
            on_open_callback=self.add_default_queue
        )

    def ping_core(self, interval=10):
        while not self.exiting:
            if self.core:
                self.core
                user = CurrentUser().fetch()
                if not user:
                    self.core = None
            if not self.core:
                self.connect_to_core()
            sleep(interval)


    def start_ping_thread(self):
        """
        Starts a separate thread `self.core_ping_thread` that keeps requesting
        """
        logging.info("Starting ping thread")
        self.core_ping_thread = Thread(target=self.ping_core)
        self.core_ping_thread.start()

    def _create_kubernetes_job(self, job: Software, message: dict) -> client.V1Job:
        """
        Creates a kubernetes `client.V1Job` from a cytomine `Software`
        specification.
        """
        job_name = kube_job_label(job)

        container = client.V1Container(
            name=job_name,
            image=message['pullingCommand'],
            args=message['command'])

        template = client.V1beta1JobTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels={"app": "cytomine"}),
                spec=client.V1PodSpec(
                    restart_policy='Never',
                    containers=[container],
                    service_account=self.settings.serviceaccount
                ))
        spec=client.V1JobSpec(template=template)
        kube_job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=spec)

        return kube_job

    def _kill_job(self, message):
        """
        Deletes the job specified in the message
        """
        logging.info("Killing job %i", message['jobId'])

        job = Job().fetch(message['jobId'])
        job_name = kube_job_label(job)
        self.jobs_api.delete_namespaced_job(job_name, "default",
            propagation_policy='Background')
        job.status = Job.TERMINATED
        job.update()

    def _run_job(self, message):
        """
        Runs a kubernetes job as defined by a cytomine job message.
        """
        logging.info("Starting job %i", message['jobId'])
        logging.info("message: %s", message)

        # Fetch the job data from core
        job = Job().fetch(message['jobId'])

        # accept the job
        job.status = Job.INQUEUE
        job.update()

        # create a kubernetes job from the job spec
        try:
            kube_job = self._create_kubernetes_job(job, message)
        except ApiException as e:
            logging.error("Couldn't create kubernetes job: %s", e)
            return

        # start the job
        try:
            job_spec = self.jobs_api.create_namespaced_job("default", kube_job)
        except ApiException as e:
            logging.error("Couldn't create kubernetes job: %s", e)
            return
        job_name = job_spec.metadata.name

        # check job status until the job finishes
        while True:
            try:
                status = self.jobs_api.read_namespaced_job_status(job_name,
                    "default")
            except ApiException as e:
                logging.error("Couldn't read job status: %s", e)
                break
            if status.status.completion_time:
                job.progress = 100
                job.status = Job.SUCCESS
                job.update()
                break
            elif status.status.failed:
                job.status = Job.FAILED
                job.update()
                break
            elif job.status != Job.RUNNING and status.status.start_time:
                job.status = Job.RUNNING
                job.update()
            time.sleep(.5)

        # send log if the job was successful
        if job.status == job.SUCCESS:
            with tempfile.TemporaryDirectory() as tempdir:

                # get uid for the job:
                job_uid = status.metadata.labels['controller-uid']
                # get pods from the uid
                pod_label_selector=f"controller-uid={job_uid}"
                pods = self.core_api.list_namespaced_pod(namespace="default",
                  label_selector=pod_label_selector)
                pod_name=pods.items[0].metadata.name
                # get log from the first pod
                log = self.core_api.read_namespaced_pod_log(pod_name, "default")

                filename = os.path.join(tempdir, 'log.out')
                with open(filename, 'w') as result:
                    # write the kubernetes log to the result file
                    result.write(log)
                    result.flush()
                    AttachedFile(job, filename).save()
                    job.update()
        else:
            job.status = Job.FAILED
            job.update()

        try:
            self.jobs_api.delete_namespaced_job(job_name, "default",
                propagation_policy='Background')
        except ApiException as e:
            logging.error("Couldn't create kubernetes job: %s", e)

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
        self.add_queue(self.settings.rabbitmq.queue,
                       self.queue_callback)

    def add_repo(self, repo_name, prefix="S_", user=None, password=None):
        """
        Adds a github software repository to the list of repositories
        """
        repo = Repository(repo_name, prefix, user, password, dev=True)
        repo.load_software()
        self.repositories[repo_name] = repo

    def connect_to_core(self):
        """
        Connects to cytomine core and sets `self.core` to a cytomine object, or
        `None` if the connection failed.
        """
        logging.info("Connecting to cytomine core")
        try:
            core = cytomine.Cytomine(
                host=self.settings.core.url,
                public_key=self.settings.core.public_key,
                private_key=self.settings.core.private_key,
                protocol=self.settings.core.protocol,
                logging_handlers=logging.getLoggerClass().root.handlers
            )
            if core.current_user:
                self.core = core
                self.get_core_id()
                self.update_core_software()
                return
        except JSONDecodeError as e:
            logging.error("Failed to parse settings file: %s", e)
        except requests.exceptions.ConnectionError as e:
            logging.error("Failed to connect: %s", e)
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
                host=self.settings.rabbitmq.host,
                port=self.settings.rabbitmq.port,
                credentials=pika.PlainCredentials(
                    self.settings.rabbitmq.username,
                    self.settings.rabbitmq.password)
            )
            self.rabbitmq = pika.SelectConnection(connection_params,
                on_open_callback=self._create_channel
            )
        except pika.exceptions.AMQPConnectionError:
            logging.error("Couldn't connect to rabbitmq")
            self.rabbitmq = None

    def get_core_id(self):
        """
        Checks the software router list in `self.core` to get own id.
        """
        servers = self.core.get_collection(ProcessingServerCollection())
        for server in servers:
            if server.name == self.name:
                self.id = server.id
                logging.info("Setting server id: %s", self.id)
                break

    def on_job(self, ch, method, properties, body):
        """
        Handles cytomine job requests.
        """
        # TODO: make a proper message parser with content checking
        msg = json.loads(body)
        ch.basic_ack(method.delivery_tag)
        if msg['requestType'] == 'execute':
            self._run_job(msg)

        elif msg['requestType'] == 'kill':
            self._kill_job(msg)

    def queue_callback(self, ch, method, properties, body):
        """
        Callback function that handles rabbitmq messages for the software router.
        """
        # TODO: make a proper message parser with content checking
        msg = json.loads(body)

        # reject messages with the wrong server id
        if msg.get('processingServerId', -1) != self.id:
            logging.warning("Rejected processing server due to wrong id")
            ch.basic_nack(method.delivery_tag)
            return

        # otherwise, add a processing server
        if msg.get('requestType', None) == 'addProcessingServer':
            logging.info("new processing server: %s", msg['name'])
            self.add_queue(msg['name'], self.on_job)

        ch.basic_ack(method.delivery_tag)

    def update_core_software(self):
        """
        Compares the software stored in `self.repositories` with that in
        `self.core`.
        """
        if not self.id:
            logging.warning("Can't add software without processing server id")
            return

        logging.info("Updating core software")
        current_software = self.core.get_collection(SoftwareCollection())
        software_names =  [s.name for s in current_software]
        for _, repo in self.repositories.items():
            for name, data in repo.software.items():

                if name not in software_names: # software does not exist already

                    data['algorithm']['defaultProcessingServer'] = self.id
                    software = Software(**data['algorithm'])
                    software.save()
                    for param in data['params']:
                        param['id_software'] = software.id
                        software_param = SoftwareParameter(**param)
                        software_param.save()
                    logging.info("%s: Added", name)
                else:
                    logging.info("%s: Already available", name)
                    # TODO: Check if software needs update

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

        # Don't connect to rabbitmq until there is a connection to core. This is
        # mainly because core tends to crash if the main queue is created before
        # it starts.
        if software_router.core and not software_router.rabbitmq:
            software_router.connect_to_rabbitmq()

            # Start the rabbitmq ioloop
            if software_router.rabbitmq:
                software_router.start()

        # sleep for a bit before checking again
        time.sleep(10)
