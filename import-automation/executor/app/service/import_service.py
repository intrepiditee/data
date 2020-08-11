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
Data Commons importer client.
"""

import os
import time
import dataclasses
from typing import Dict

import requests
from google.cloud import storage

from app.executor import import_target

_PROXY_HOST = 'https://datcom-api-key-sandbox.uc.r.appspot.com'
_PROXY_IMPORT_TABLE = f'{_PROXY_HOST}/ImportTable'
_PROXY_IMPORT_NODE = f'{_PROXY_HOST}/ImportNode'
_PROXY_GET_IMPORT_LOG = f'{_PROXY_HOST}/GetImportLog'
_PROXY_IMPORT_DELETE = f'{_PROXY_HOST}/ImportDelete'


@dataclasses.dataclass
class ImportInputs:
    # Path to the CSV, relative to the root directory of the bucket in which
    # the CSV is stored, e.g.,
    # scripts/us_fed/treasury/2020_07_15T12_07_17_365264_07_00/data.csv
    cleaned_csv: str = None
    # Path to the template MCF, relative to the root directory of the bucket
    template_mcf: str = None
    # Path to the node MCF, relative to the root directory of the bucket
    node_mcf: str = None


class PreviousImportNotFinishedError(Exception):
    """Exception thrown if an import with the same import name and the
    same email is still queued or running.
    
    Attributes:
        import_name: Import name submitted to the importer as a string.
        curator_email: Email submitted to the importer as a string.
    """

    def __init__(self, import_name: str, curator_email: str):
        super().__init__(f'Previous import {import_name} ({curator_email}) '
                         'is still queued or running')
        self.import_name = import_name
        self.curator_email = curator_email


class ImportNotFoundError(Exception):
    """Exception thrown if an import is expected to be found in the import
    logs but not.

    Attributes:
        import_name: Import name submitted to the importer as a string.
        curator_email: Email submitted to the importer as a string.
    """

    def __init__(self, import_name: str, curator_email: str):
        super().__init__(f'Import {import_name} ({curator_email}) '
                         'not found in import logs')
        self.import_name = import_name
        self.curator_email = curator_email


class ImportServiceClient:
    """Data Commons importer client."""
    # Number of seconds between each get_import_log call for
    # blocking on imports.
    _SLEEP_DURATION: float = 60
    # Enum value for ImportLogEntry.BIGQUERY
    _BIGQUERY = 2
    # Enum value for ResolutionNeed.LOCAL_RES_BY_ID
    _LOCAL_RES_BY_ID = 2
    # Enum value for NodeFormat.MCF
    _MCF = 1

    def __init__(self, project_id: str, unresolved_mcf_bucket_name: str,
                 resolved_mcf_bucket_name: str, importer_output_prefix: str,
                 executor_output_prefix: str):
        """Constructs an ImportServiceClient.
        
        Args:
            project_id: project_id: ID of the Google Cloud project that hosts
                the bucket, as a string.
            unresolved_mcf_bucket_name: Name of the Cloud Storage bucket the
                the generated data files are uploaded to, as a string.
            resolved_mcf_bucket_name: Name of the Cloud Storage bucket the
                Data Commons importer outputs to, as a string.
            importer_output_prefix: Output prefix in the
                unresolved_mcf_bucket_name bucket, as a string.
            executor_output_prefix: Output prefix in the
                resolved_mcf_bucket_name bucket, as a string.
        """
        client = storage.Client(project=project_id)
        self.unresolved_bucket = client.bucket(unresolved_mcf_bucket_name)
        self.resolved_bucket = client.bucket(resolved_mcf_bucket_name)
        self.unresolved_mcf_bucket_name = unresolved_mcf_bucket_name
        self.resolved_mcf_bucket_name = resolved_mcf_bucket_name
        self.importer_output_prefix = importer_output_prefix
        self.executor_output_prefix = executor_output_prefix

    def smart_import(self,
                     import_dir: str,
                     import_inputs: ImportInputs,
                     import_spec: dict,
                     block: bool = False,
                     timeout: float = None) -> Dict:
        """Performs a table import or node import depending on the
        import_inputs parameter.

        In import_inputs, if cleaned_csv and template_mcf are both set, performs
        a table import. If only node_mcf is set, performs a node import.

        Args:
            import_dir: Path to the directory with the manifest, relative to
                the root directory of the repository, as a string.
            import_inputs: ImportInputs object containing input paths.
            import_spec: Import specification as a dict.
            block: Whether to block the calling thread until the import succeeds
                or fails, as a boolean.
            timeout: If block is set, the maximum time in seconds to block, as
                a float.

        Returns:
            The response from the importer as a dict.
        
        Raises:
            ValueError: import_inputs does not match any condition, i.e., no
                import performed.
            requests.HTTPError: The importer returns a status code that is
                larger than or equal to 400.
            PreviousImportNotFinishedError: An import with the same absolute
                import name and the same email as the first email in the
                curator_email list in the import specification is still queued
                or running.
        """
        if import_inputs.cleaned_csv and import_inputs.template_mcf:
            return self.import_table(import_dir, import_inputs, import_spec,
                                     block, timeout)
        if import_inputs.node_mcf and not import_inputs.cleaned_csv and not import_inputs.template_mcf:
            return self.import_node(import_dir, import_inputs, import_spec,
                                    block, timeout)
        raise ValueError(f'Invalid import inputs {import_inputs}')

    def import_table(self,
                     import_dir: str,
                     import_inputs: ImportInputs,
                     import_spec: dict,
                     block: bool = False,
                     timeout: float = None) -> Dict:
        """Imports a CSV table with template MCF and node MCF.
        See smart_import."""
        absolute_import_name = _get_fixed_absolute_import_name(
            import_dir, import_spec['import_name'])
        curator_email = import_spec['curator_emails'][0]

        if not self._import_finished(absolute_import_name, curator_email):
            raise PreviousImportNotFinishedError(absolute_import_name,
                                                 curator_email)

        request = {
            'manifest': {
                'import_name': absolute_import_name,
                'curator_email': curator_email,
                'table': {
                    'table_name':
                        absolute_import_name,
                    'csv_path':
                        self._fix_input_path(import_inputs.cleaned_csv),
                    'mapping_path':
                        self._fix_input_path(import_inputs.template_mcf),
                    'field_delim':
                        ','
                },
                'provenance_url': import_spec['provenance_url'],
                'provenance_description': import_spec['provenance_description'],
                'mcf_url': [self._fix_input_path(import_inputs.node_mcf)],
            },
            'use_flume': False,
            'db_type': ImportServiceClient._BIGQUERY,
            'resolution_need': ImportServiceClient._LOCAL_RES_BY_ID,
            'skip_csv_value_checks': True,
            'write_to_db': True
        }
        response = requests.post(_PROXY_IMPORT_TABLE, json=request)
        response.raise_for_status()

        if block:
            self._block_on_import(absolute_import_name, curator_email, timeout)

        return response.json()

    def import_node(self,
                    import_dir: str,
                    import_inputs: ImportInputs,
                    import_spec: dict,
                    block: bool = False,
                    timeout: float = None) -> Dict:
        """Imports a node MCF file. See smart_import."""
        absolute_import_name = _get_fixed_absolute_import_name(
            import_dir, import_spec['import_name'])
        curator_email = import_spec['curator_emails'][0]

        if not self._import_finished(absolute_import_name, curator_email):
            raise PreviousImportNotFinishedError(absolute_import_name,
                                                 curator_email)

        request = {
            'node_file_pattern': self._fix_input_path(import_inputs.node_mcf),
            'node_format': ImportServiceClient._MCF,
            'import_name': absolute_import_name,
            'use_flume': False,
            'sanity_check_only': False,
            'user_email': curator_email,
            'provenance_url': import_spec['provenance_url'],
            'provenance_description': import_spec['provenance_description'],
            'db_type': ImportServiceClient._BIGQUERY,
            'resolution_need': ImportServiceClient._LOCAL_RES_BY_ID,
            'write_to_db': True,
            'generate_dcids_for_new_places': False
        }
        response = requests.post(_PROXY_IMPORT_NODE, json=request)
        response.raise_for_status()

        if block:
            self._block_on_import(absolute_import_name, curator_email, timeout)

        return response.json()

    def delete_import(self,
                      import_dir: str,
                      import_spec: Dict,
                      block: bool = False,
                      timeout: float = None) -> Dict:
        """Deletes an import.

        Args:
            import_dir: Path to the directory containing the manifest for the
                import, relative to the root directory of the repository.
            import_spec: Specification of the import parsed from the manifest.
            block: Whether to block the calling thread until the deletion is
                failed or finished.
            timeout: If block is set, the maximum time to block in seconds.
        
        Returns:
            The response from the importer as a dict.

        Raises:
            requests.HTTPError: The importer returns a status code that is
                larger than or equal to 400.
        """
        absolute_import_name = _get_fixed_absolute_import_name(
            import_dir, import_spec['import_name'])
        curator_email = import_spec['curator_emails'][0]
        request = {
            'import_name': absolute_import_name,
            'user_email': curator_email,
            'db_type': ImportServiceClient._BIGQUERY
        }
        response = requests.post(_PROXY_IMPORT_DELETE, json=request)
        response.raise_for_status()

        if block:
            self._block_on_import(absolute_import_name,
                                  curator_email,
                                  timeout,
                                  delete=True)

        return response.json()

    def delete_previous_output(self, import_dir: str,
                               import_spec: dict) -> None:
        """Deletes artifacts generated by the previous import, such as
        the resolved MCF.

        Args:
            import_dir: Path to the directory containing the manifest for the
                import, relative to the root directory of the repository.
            import_spec: Specification of the import parsed from the manifest.
        """
        absolute_import_name = _get_fixed_absolute_import_name(
            import_dir, import_spec['import_name'])

        for bucket in (self.unresolved_bucket, self.resolved_bucket):
            blobs = bucket.list_blobs(prefix=os.path.join(
                self.importer_output_prefix, absolute_import_name))
            for blob in blobs:
                blob.delete()

    def get_import_log(self, curator_email: str) -> Dict:
        """Gets import logs.
        
        Args:
            curator_email: Email submitted to the importer.
        
        Returns:
            The response from the importer as a dict.

        Raises:
            requests.HTTPError: The importer returns a status code that is
                larger than or equal to 400.
        """
        request = {'user_email': curator_email}
        response = requests.post(_PROXY_GET_IMPORT_LOG, json=request)
        response.raise_for_status()
        return response.json()

    def _fix_input_path(self, path: str) -> str:
        return os.path.join('/bigstore', self.unresolved_mcf_bucket_name,
                            self.executor_output_prefix, path)

    def _block_on_import(self,
                         import_name: str,
                         curator_email: str,
                         timeout: float = None,
                         delete: bool = False) -> None:
        """Blocks the calling thread until the import fails or succeeds.

        Args:
            import_name: Import name submitted to the importer.
            curator_email: User email submitted to the importer.
            timeout: Maximum time to block in seconds.
            delete: Whether to block on an import or the deletion of an import.

        Raises:
            Same exceptions as ImportServiceClient.get_import_log.
            TimeoutError: Timeout expired.
        """
        start = time.time()
        while True:
            if self._import_finished(import_name, curator_email, delete):
                return
            if timeout is not None and time.time() - start > timeout:
                raise TimeoutError('Timeout expired blocking on '
                                   f'{import_name} ({curator_email})')
            time.sleep(ImportServiceClient._SLEEP_DURATION)

    def _import_finished(self,
                         import_name: str,
                         curator_email: str,
                         delete: bool = False) -> bool:
        """Checks if an import has finished (succeeded or failed).

        Pairs of <import_name, curator_email> are not unique on the importer
        side. This function returns False if there is any import with the
        import_name and curator_email that has not finished.

        Args:
            import_name: Import name submitted to the importer.
            curator_email: Email submitted to the importer.
            delete: Whether to the job is a deletion.

        Returns:
            Whether the import has finished.

        Raises:
            Same exceptions as get_import_log.
            ImportNotFoundError: Import not found in the import logs.
        """
        logs = self.get_import_log(curator_email)
        found = False
        finished = True
        for log in logs['entry']:
            if (log['userEmail'] == curator_email and
                    log['importName'] == import_name):
                # For a deletion job, log['stages'] is ['DELETE'].
                # For a table import job, log['stages'] is something like
                # ["TABLE2MCF", "LOCAL_RESOLVE_BY_ID", "WRITE"].
                # For a node import job, log['stages'] is something like
                # ["LOCAL_RESOLVE_BY_ID", "WRITE"].
                # The resolution types may vary.
                if delete == (log['stages'] == ['DELETE']):
                    found = True
                    if log['state'] in ('QUEUED', 'RUNNING'):
                        finished = False
                        break
        if not found:
            raise ImportNotFoundError(import_name, curator_email)
        return finished


def _get_fixed_absolute_import_name(import_dir: str, import_name: str) -> str:
    """Returns the absolute import name with colons and backslashes
    replaced with underscores."""
    return _fix_absolute_import_name(
        import_target.get_absolute_import_name(import_dir, import_name))


def _fix_absolute_import_name(name: str) -> str:
    """Replaces colons and backslashes with underscores."""
    return name.replace(':', '_').replace('/', '_')
