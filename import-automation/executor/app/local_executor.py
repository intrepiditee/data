from absl import flags
from absl import app

from app import utils
from app.executor import executor
from app.service import gcs_io

_IMPORT_NAME_FORM = '''
<path to the directory containing the manifest>:<import name>
'''.strip()

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'import_name',
    None,
    'Absolute import name of the import to execute of the form '
    f'{_IMPORT_NAME_FORM}.',
    short_name='i')
flags.DEFINE_string(
    'output_dir',
    '.',
    'Path to the output directory.',
    short_name='o')
flags.mark_flag_as_required('import_name')
flags.register_validator('import_name',
                         utils.absolute_import_name,
                         message='--import_name must be of the form '
                                 f'{_IMPORT_NAME_FORM}.')


def main(_):
    results = executor.execute_imports_on_update(
        absolute_import_name=FLAGS.import_name,
        bucket_io=gcs_io.LocalBucketIO(dir_path=FLAGS.output_dir))
    print(results)


if __name__ == '__main__':
    app.run(main)
