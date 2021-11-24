#!/usr/bin/env python3
"""
The repository class loads selected repositories from github as cytomine
software.
"""

import json
import logging

from github import Github, GithubException, RateLimitExceededException


class Repository():

    def __init__(self, name, prefix="S_", user=None, password=None, dev=False):
        """
        Sets repository values, and connects to github.
        """
        self.name = name
        self.prefix = prefix
        self.user = user
        self.password = password
        self.connection = None

        # if set to true, this will only load a static version of the
        # "python_software_example", to not exceed the github API limit while
        # developing
        self.dev_mode = dev
        if self.dev_mode:
            logging.warning(("dev mode set for repository. "
                             "No real data will be loaded."))

        self.software = {}

        if not self.dev_mode:
            self.connect()

    def _load_dev_software(self):
        """
        Loads the python_software_example from a static file instead of loading
        it from github.
        """
        logging.info("Loading development example software")
        attributes = json.loads(open("example.json").read())
        software = self._parse_software_attributes(**attributes)
        software['algorithm']['softwareVersion'] = "example"
        software['algorithm']['pullingCommand'] = \
            f"{attributes['container-image']['image']}:v1.0"
        self.software[software['algorithm']['name']] = software

    def _parse_software_attributes(self, **attributes):
        """
        Formats software attributes loaded from the `descriptor.json` in
        cytomine software repos into a format that's suitable to be loaded by
        the cytomine Software class.
        """

        # Handle inputs first

        params = []
        # add params
        for param in attributes.get('inputs', []):
            # I think all of these are correct, but there are a bunch of
            # guesswork here.
            parameter = {
                "name": param.get('id', ""),
                "type": param.get('type', None),
                "set_by_server": param.get('set-by-server', None),
                "required": not param.get('optional', True),
                "default_value": param.get('default-value', None),
                "human_name": param.get('description', None),
                "command_line_flag": param.get('command-line-flag', None),
                "value_key": param.get('value-key', None),
                "uri": param.get('uri', None),
                # Note that "attribut" is not a typo here
                "uri_sort_attribut": param.get('uri-sort-attribute', None),
                "uri_print_attribut": param.get('uri-print-attribute', None),
            }
            # handle convenience variables
            # TODO: handle as a general case.
            if parameter['value_key'] == "[@ID]":
                parameter['value_key'] = f"[{param.get('id', '').upper()}]"
            if parameter['command_line_flag'] == "--@id":
                parameter['command_line_flag'] = \
                    f"--{param.get('id', '').lower()}"

            params += [parameter]

        # Then create the algorithm

        algorithm = attributes

        algorithm['executeCommand'] = attributes["command-line"]
        algorithm['executable'] = True
        algorithm['inputs'] = None

        return {'algorithm': algorithm, 'params': params}

    def connect(self):
        """
        Connects to github
        """
        logging.info("Connecting to github")
        username = self.name
        password = self.password

        self.connection = Github(username, password)

    def load_software(self):
        """
        Loads all github repos from the github user in `self.name` whose name
        starts with `self.prefix` as cytomine software.
        """
        if self.dev_mode:
            self._load_dev_software()
            return

        logging.info("Loading software from github.com/%s", self.name)

        try:
            user = self.connection.get_user(self.name)
            repos = [r for r in user.get_repos() if r.name.startswith(self.prefix)]
        except (RateLimitExceededException, GithubException) as e:
            logging.error(e)
            return

        for i, repo in enumerate(repos):
            try:
                logging.info("Loading %s", repo.name)
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

                software = self._parse_software_attributes(**attributes)

                # update some software values
                # There aren't that many fields in the Software class, and since
                # we don't need the pullingCommand when running in kubernetes we
                # use it to hold image name (which we do need).
                software['algorithm']['softwareVersion'] = release.title
                software['algorithm']['pullingCommand'] = \
                    f"{attributes['container-image']['image']}:{tag.name}"

                self.software[software['algorithm']['name']] = software
            except RateLimitExceededException as e:
                logging.warning(e)
            except GithubException as e:
                logging.error(e)
