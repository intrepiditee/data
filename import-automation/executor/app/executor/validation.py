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

from app import utils
from app.executor import import_executor
from app.executor import import_target

TASK_INFO_REQUIRED_FIELDS = ['REPO_NAME', 'BRANCH_NAME', 'COMMIT_SHA', 'PR_NUMBER']
MANIFEST_REQUIRED_FIELDS = []
IMPROT_SPEC_REQUIRED_FIELDS = []


def import_targets_valid(import_targets, manifest_dirs):
    import_all = 'all' in import_targets
    if not import_targets:
        raise ValueError('No import target specified in commit message')
    relative_names = import_target.get_relative_import_names(import_targets)
    if not import_all and len(manifest_dirs) > 1 and relative_names:
        err_template = 'Commit touched multiple directories {} but {} are relative import names'
        err = err_template.format(
            utils.list_to_str(manifest_dirs), utils.list_to_str(relative_names))
        raise ValueError(err)


def import_spec_valid(import_spec):
    pass


def manifest_valid(manifest):
    pass


def task_info_valid(task_info):
    missing_keys = get_missing_keys(TASK_INFO_REQUIRED_FIELDS, task_info)
    if missing_keys:
        return False, 'Missing {} in task info'.format(', '.join(missing_keys))

    return True, ''


def get_missing_keys(keys, a_dict):
    return list(key for key in keys if key not in a_dict)
