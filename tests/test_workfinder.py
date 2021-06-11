#!/usr/bin/env python

"""Tests for `workfinder` package."""


import unittest
import pandas as pd

from workfinder import workfinder
from workfinder.search import BaseWorkFinder


class TestWorkfinder(unittest.TestCase):
    """Tests for `workfinder` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_matching_empty(self):
        """given a work list and done list find the things in the work list that are not in the done list."""
        work_list = pd.DataFrame({'id': [], 'url': []})
        done_list = pd.DataFrame({'id': []})

        result = BaseWorkFinder._diff_work_lists(work_list, done_list)

        assert result.empty

    def test_matching_no_new(self):
        work_list = pd.DataFrame({'id': [], 'url': []})
        done_list = pd.DataFrame({'id': ["123", "456"]})
        result = BaseWorkFinder._diff_work_lists(work_list, done_list)

        assert result.empty

    def test_matching_with_stuff(self):
        work_list = pd.DataFrame({'id': ["789"], 'url': ["foo.bar.com/blah.zip"]})
        done_list = pd.DataFrame({'id': ["123", "456"]})
        result = BaseWorkFinder._diff_work_lists(work_list, done_list)

        assert result.size == 2
        assert result['id'][0] == "789"
        assert result['url'][0] == "foo.bar.com/blah.zip"
