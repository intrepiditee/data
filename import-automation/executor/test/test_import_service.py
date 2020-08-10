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
"""
Tests for import_executor.py.
"""

import unittest
from unittest import mock

from app.service import import_service

_CLIENT = 'app.service.import_service.ImportServiceClient'


class ImportServiceTest(unittest.TestCase):

    def setUp(self):
        self.importer = import_service.ImportServiceClient(
            'project_id', 'unresolved_mcf_bucket_name',
            'resolved_mcf_bucket_name', 'importer_output_prefix',
            'executor_output_prefix')

    def test_fix_input_path(self):
        expected = ('/bigstore/unresolved_mcf_bucket_name/'
                    'executor_output_prefix/foo/bar/data.csv')
        self.assertEqual(expected,
                         self.importer._fix_input_path('foo/bar/data.csv'))

    @mock.patch(f'{_CLIENT}.import_table')
    @mock.patch(f'{_CLIENT}.import_node')
    def test_smart_import(self, import_node, import_table):
        self.importer.smart_import('import_dir',
                                   import_service.ImportInputs(
                                       cleaned_csv='import.csv',
                                       template_mcf='import.tmcf',
                                       node_mcf='import.mcf'),
                                   import_spec={})
        import_table.assert_called_once()

        self.importer.smart_import(
            'import_dir',
            import_service.ImportInputs(node_mcf='import.mcf'),
            import_spec={})
        import_node.assert_called_once()

        self.assertRaises(ValueError,
                          self.importer.smart_import,
                          'import_dir',
                          import_service.ImportInputs(cleaned_csv='import.csv',
                                                      node_mcf='import.mcf'),
                          import_spec={})

    @mock.patch(f'{_CLIENT}._import_finished')
    @mock.patch(f'{_CLIENT}._SLEEP_DURATION', 0.00001)
    def test_block_on_import(self, import_finished):
        import_finished.side_effect = [False, False, False, True]
        self.importer._block_on_import('import_name',
                                       'curator_email',
                                       timeout=1)
        import_finished.side_effect = [False, False, False, True]
        self.assertRaises(TimeoutError,
                          self.importer._block_on_import,
                          'import_name',
                          'curator_email',
                          timeout=0)

    @mock.patch(f'{_CLIENT}.get_import_log')
    def test_import_finished(self, get_import_log):
        get_import_log.return_value = {
            'entry': [{
                'userEmail': 'email1',
                'importName': 'name1',
                'stages': ["TABLE2MCF", "LOCAL_RESOLVE_BY_ID", "WRITE"],
                'state': 'QUEUED'
            }, {
                'userEmail': 'email2',
                'importName': 'name2',
                'stages': ["TABLE2MCF", "LOCAL_RESOLVE_BY_ID", "WRITE"],
                'state': 'SUCCESSFUL'
            }, {
                'userEmail': 'email',
                'importName': 'name',
                'stages': ['DELETE'],
                'state': 'QUEUED'
            }, {
                'userEmail': 'email',
                'importName': 'name',
                'stages': ['DELETE'],
                'state': 'SUCCESSFUL'
            }, {
                'userEmail': 'email',
                'importName': 'name',
                'stages': ["TABLE2MCF", "LOCAL_RESOLVE_BY_ID", "WRITE"],
                'state': 'RUNNING'
            }]
        }
        self.assertRaises(import_service.ImportNotFoundError,
                          self.importer._import_finished,
                          'name1',
                          'email1',
                          delete=True)
        self.assertFalse(
            self.importer._import_finished('name', 'email', delete=True))
        self.assertFalse(self.importer._import_finished('name', 'email'))
        self.assertTrue(self.importer._import_finished('name2', 'email2'))

    def test_get_fixed_absolute_import_name(self):
        self.assertEqual(
            'foo_bar_treasury_import',
            import_service._get_fixed_absolute_import_name(
                'foo/bar', 'treasury_import'))
