import unittest
from unittest import mock
import subprocess

from app.executor import import_executor


class ImportExecutorTest(unittest.TestCase):
    def test_construct_process_message(self):
        process = subprocess.run(
            'printf "out" & >&2 printf "err" & exit 1',
            shell=True, text=True, capture_output=True)
        message = import_executor._construct_process_message('message', process)
        expected = (
            'message\n'
            '[Subprocess command]: printf "out" & >&2 printf "err" & exit 1\n'
            '[Subprocess return code]: 1\n'
            '[Subprocess stdout]:\n'
            'out\n'
            '[Subprocess stderr]:\n'
            'err')
        self.assertEqual(expected, message)

    def test_construct_process_message_no_output(self):
        """Tests that _construct_process_message does not append
        empty stdout and stderr to the message."""
        process = subprocess.run(
            'exit 0',
            shell=True, text=True, capture_output=True)
        message = import_executor._construct_process_message('message', process)
        expected = (
            'message\n'
            '[Subprocess command]: exit 0\n'
            '[Subprocess return code]: 0')
        self.assertEqual(expected, message)
