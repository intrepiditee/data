import os
import shutil

from google.cloud import storage

from app import configs


class BucketIO:
    def __init__(self, path_prefix=''):
        self.prefix = path_prefix

    def upload_file(self, src, dest):
        raise NotImplementedError

    def update_version(self, version):
        raise NotImplementedError

    def _get_dest(self, dest):
        return f'{self.prefix}/{dest}'


class GCSBucketIO(BucketIO):
    def __init__(self, path_prefix='', bucket_name=None, bucket=None, client=None):
        super().__init__(path_prefix)
        if not bucket_name:
            bucket_name = configs.BUCKET_NAME
        if not client:
            client = storage.Client()
        if not bucket:
            bucket = client.bucket(bucket_name)
        self.client = client
        self.bucket = bucket
        self.prefix = path_prefix

    def upload_file(self, src, dest):
        """Uploads a file to the bucket.

        Args:
            src: Path to the file to upload, as a string.
            dest: Destination in the bucket as a string. This will be prefixed
                by the prefix attribute.
        """
        blob = self.bucket.blob(os.path.join(self.prefix, dest))
        blob.upload_from_filename(src)

    def update_version(self, version):
        """Updates the version file in the bucket at
        <prefix>/latest_version.txt.

        Args:
            version: New version as a string
        """
        blob = self.bucket.blob(os.path.join(self.prefix, 'latest_version.txt'))
        blob.upload_from_string(version)


class LocalBucketIO(BucketIO):
    def __init__(self, path_prefix='', dir_path=''):
        super().__init__(path_prefix)
        self.dir_path = dir_path

    def upload_file(self, src, dest):
        shutil.copyfile(src, self._get_dest(dest))

    def update_version(self, version):
        with open(self._get_dest('latest_version.txt'), 'w') as out:
            out.write(version)

    def _get_dest(self, dest):
        return f'{self.dir_path}/{super()._get_dest(dest)}'
