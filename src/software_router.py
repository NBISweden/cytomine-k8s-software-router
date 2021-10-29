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
from types import FunctionType

import yaml

import pika
import cytomine
from cytomine.models import Software, SoftwareParameter
from cytomine.models.software import Job, Software
from cytomine.models.property import AttachedFile
from github import Github, GithubException
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from json.decoder import JSONDecodeError

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

    def __init__(self, settings):
        """
        Reads a yaml settings file `settings`.
        """
        self.settings = self._load_settings(settings)

        self.core = None
        self.server = None
        self.rabbitmq = None
        self.github = None

        # kubernetes job stuff
        self.kube_config = config.load_incluster_config()
        self.jobs_api = client.BatchV1Api()
        self.core_api = client.CoreV1Api()

        self.channel = None

    def _connect_to_github(self):
        """
        Connects to github
        """
        logging.info("Creating github connection")
        username = self.settings['github']['username']
        password = self.settings['github']['password']

        self.github = Github(username, password)

    def _create_channel(self, connection):
        """
        Creates the rabbitmq communication channel
        """
        self.channel = self.rabbitmq.channel(
            on_open_callback=self.add_default_queue
        )

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
                    service_account=self.settings['serviceaccount']
                ))
        spec=client.V1JobSpec(template=template)
        kube_job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=spec)

        return kube_job

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
                protocol=self.settings['core'].get('protocol', None),
                logging_handlers=logging.getLoggerClass().root.handlers
            )
            if core.current_user:
                self.core = core
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
        logging.info("Received: %s", msg)
        if msg.get('requestType', None) == 'addProcessingServer':
            logging.info("new processing server: %s", msg['name'])
            self.add_queue(msg['name'], self.on_job)
            software_router.update_software("cytomine")

        ch.basic_ack(method.delivery_tag)

    def add_fake_software(self):
        """
        Adds a fake testing-job to the system. This can be useful when debugging
        the system instead of loading the real software from github.
        """
        logging.info("Adding fake software 'test'")
        # There aren't that many fields in the Software class, and since we
        # don't need the pullingCommand when running in kubernetes we use it
        # to hold image name.
        algorithm = Software(
            name="test",
            fullName="Fake Testing Job",
            softwareVersion="v1.0",
            defaultProcessingServer=101, # important, makes the jobs come here
            Parameters=[],
            executable=True,
            executeCommand="echo [TEST_PARAM]",
            pullingCommand="busybox:latest"
        )
        algorithm.save()

        SoftwareParameter(
            name="test_param",
            type="Number",
            id_software=algorithm.id,
            default_value=3,
            human_name="Test parameter",
            command_line_flag="--test-param",
            value_key="TEST_PARAM"
        ).save()

    def update_software(self, github_user, prefix="S_"):
        """
        Updates the available softwares list for the given github user and
        prefix.
        """
        logging.info("Adding software from github/%s", github_user)
        if self.github == None:
            self._connect_to_github()

        # TODO: Make checks to see if the software already exists
        user = self.github.get_user(github_user)
        for repo in [r for r in user.get_repos() if r.name.startswith(prefix)]:
            logging.info("Adding %s", repo.name)
            try:
                # Get the latest release
                release = repo.get_latest_release()
                if not release:
                    logging.warning("Not added. No releases.")
                    continue

                # get tag to get the target commit from the release
                tag = [t for t in repo.get_tags() if t.name == release.tag_name]
                if len(tag) < 0:
                    logging.warning("Not added. No release tag.")
                    continue
                tag = tag[0]

                # Get the descriptor from the commit references from the tag
                descriptor = repo.get_contents("descriptor.json",
                                               ref=tag.commit.sha)
                attributes = json.loads(descriptor.decoded_content.decode())

                # A bunch of stuff needs to be adjusted between the json and
                # attributes dict, so we try to do that here
                #attributes = parse_descriptor(file_content)
                algorithm = Software(**attributes)

                # set algorithm values
                # There aren't that many fields in the Software class, and since
                # we don't need the pullingCommand when running in kubernetes we
                # use it to hold image name.
                algorithm.softwareVersion = release.title
                algorithm.executeCommand = attributes["command-line"]
                algorithm.pullingCommand = \
                    f"{attributes['container-image']['image']}:{tag.name}"
                algorithm.defaultProcessingServer = 101
                algorithm.executable = True
                algorithm.inputs = None

                # save to create the object in core, and get an id
                algorithm.save()

                # add params
                for param in attributes['inputs']:
                    # I think all of these are correct, but there are a bunch of
                    # guesswork here.
                    parameter = SoftwareParameter(
                        name=param.get('id', ""),
                        type=param.get('type', None),
                        id_software=algorithm.id,
                        set_by_server=param.get('set-by-server', None),
                        required=not param.get('optional', True),
                        default_value=param.get('default-value', None),
                        human_name=param.get('description', None),
                        command_line_flag=param.get('command-line-flag', None),
                        value_key=param.get('value-key', None),
                        uri=param.get('uri', None),
                        # Note that "attribut" is not a typo here
                        uri_sort_attribut=param.get('uri-sort-attribute', None),
                        uri_print_attribut=param.get('uri-print-attribute',
                                                     None),
                    )
                    # handle convenience variables
                    # TODO: handle as a general case.
                    if parameter.valueKey == "[@ID]":
                        parameter.valueKey = f"[{param.get('id', '').upper()}]"
                    if parameter.commandLineFlag == "--@id":
                        parameter.commandLineFlag = f"--{param.get('id', '').lower()}"

                    parameter.save()

            except GithubException as e:
                logging.error("No descriptor.json file for %s: %s",
                              repo.name, e)

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

            # Start the rabbitmq ioloop
            if software_router.rabbitmq:
                software_router.start()

        # sleep for a bit before checking again
        time.sleep(10)
