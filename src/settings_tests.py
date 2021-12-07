#!/usr/bin/env python3
"""
Unittests for the Settings class.

TODO: implement further tests
"""

import unittest

from settings import parse_to_namespaces, Settings

class TestParseToNamespaces(unittest.TestCase):

    def test_wrong_data(self):
        with self.assertRaises(AssertionError):
            parse_to_namespaces([], "")

if __name__ == '__main__':
    unittest.main()
