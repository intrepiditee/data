# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import unittest
from unittest import mock
import datetime
import tempfile
import hashlib

import app.utils


class AppUtilsTest(unittest.TestCase):
    """Tests for app/utils.py."""

    def test_pttime_to_datetime(self):
        """Tests that the string returned by pttime can be converted to
        a datetime object and the timezone component is correct."""
        time_iso = app.utils.pttime()
        time_datetime = datetime.datetime.fromisoformat(time_iso)
        offset = time_datetime.utcoffset().total_seconds()
        self.assertTrue(offset in (-(7 * 60 * 60), -(8 * 60 * 60)))
        tzname = time_datetime.tzname()
        self.assertTrue(tzname in ('UTC-07:00', 'UTC-08:00'))

    def test_pttime_to_datetime_then_back(self):
        """Tests that the string returned by pttime can be converted to
        a datetime object and remains the same when converted back."""
        time_iso = app.utils.pttime()
        time_datetime = datetime.datetime.fromisoformat(time_iso)
        self.assertEqual(time_iso, time_datetime.isoformat())

    def test_download_file(self):
        """Response does not have a Content-Disposition header."""
        url = ('https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/'
               'pdf/dummy.pdf')
        with tempfile.TemporaryDirectory() as dest_dir:
            path = app.utils.download_file(url, dest_dir)
            with open(path, 'rb') as file:
                self.assertEqual('90ffd2359008d82298821d16b21778c5c39aec36',
                                 hashlib.sha1(file.read()).hexdigest())

    @mock.patch('requests.Response')
    def test_get_filename(self, response):
        response.headers.get.return_value = 'attachment; filename=FRB_H15.csv'
        self.assertEqual('FRB_H15.csv', app.utils._get_filename(response))
        response.headers.get.assert_called_once_with('Content-Disposition')

    @mock.patch('requests.Response')
    def test_get_filename_raise(self, response):
        response.headers.get.return_value = 'attachment'
        self.assertRaises(ValueError, app.utils._get_filename, response)
        response.headers.get.assert_called_once_with('Content-Disposition')


class TestUtilsTest(unittest.TestCase):
    """Tests for test/utils.py."""
    pass
