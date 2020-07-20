from absl import flags
from absl import app

from app import configs
from app.executor import import_target
from app.executor import import_executor
from app.service import file_uploader
from app.service import github_api

_IMPORT_NAME_FORM = '''
<path to the directory containing the manifest>:<import name>
'''.strip()

FLAGS = flags.FLAGS
flags.DEFINE_string(
    name='import_name',
    default=None,
    help='Absolute import name of the import to execute of the form '
         f'{_IMPORT_NAME_FORM}.',
    short_name='i')
flags.DEFINE_string(
    name='output_dir',
    default='.',
    help='Path to the output directory.',
    short_name='o')
flags.DEFINE_string(
    name='repo_name',
    default='data',
    help='Name of the GitHub repository containing the import.',
    short_name='r')
flags.DEFINE_string(
    name='owner_username',
    default='datacommonsorg',
    help='GitHub username of the owner of the GitHub repository '
         'containing the import.',
    short_name='u')
flags.DEFINE_string(
    name='username',
    default='',
    help='GitHub username for authentication.',
    short_name='n')
flags.DEFINE_string(
    name='access_token',
    default='',
    help='GitHub access token for authentication.',
    short_name='t')

flags.mark_flag_as_required('import_name')
flags.register_validator('import_name',
                         import_target.absolute_import_name,
                         message='--import_name must be of the form '
                                 f'{_IMPORT_NAME_FORM}.')


def main(_):
    config = configs.ExecutorConfig(
        github_repo_name=FLAGS.repo_name,
        github_repo_owner_username=FLAGS.owner_username,
        github_auth_username=FLAGS.username,
        github_auth_access_token=FLAGS.access_token)
    executor = import_executor.ImportExecutor(
        uploader=file_uploader.LocalFileUploader(output_dir=FLAGS.output_dir),
        github=github_api.GitHubRepoAPI(
            config.github_repo_owner_username, config.github_repo_name),
        config=config)
    results = executor.execute_imports_on_update(FLAGS.import_name)
    print(results)


if __name__ == '__main__':
    app.run(main)
