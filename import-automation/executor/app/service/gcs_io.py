import os
import shutil

from google.cloud import storage

from app import configs


class BucketIO:
    def __init__(self, prefix=''):
        self.prefix = prefix

    def upload_file(self, src, dest):
        raise NotImplementedError

    def update_version(self, version):
        raise NotImplementedError

    def _get_dest(self, dest):
        return f'{self.prefix}/{dest}'


class GCSBucketIO(BucketIO):

    def __init__(self, prefix='', bucket_name=configs.BUCKET_NAME, bucket=None, client=None):
        super().__init__(prefix)
        if not client:
            client = storage.Client()
        if not bucket:
            bucket = client.bucket(bucket_name)
        self.client = client
        self.bucket = bucket

    # def download_dir(self, src, dest):
    #     os.makedirs(dest, exist_ok=True)
    #     for blob in self.bucket.list_blobs(prefix=src):
    #         file_dest = os.path.join(dest, os.path.basename(blob.name))
    #         self.download_file(blob.name, file_dest)
    #
    # def download_file(self, src, dest):
    #     os.makedirs(os.path.dirname(dest), exist_ok=True)
    #     blob = self.bucket.blob(src)
    #     blob.download_to_filename(dest)

    # def upload_dir(self, src, dest):
    #     with os.scandir(src) as entry_iter:
    #         for entry in entry_iter:
    #             if entry.is_dir(follow_symlinks=False):
    #                 self.upload_dir(entry.path, os.path.join(dest, entry.name))
    #             elif entry.is_file(follow_symlinks=False):
    #                 self.upload_file(entry.path, os.path.join(dest, entry.name))

    def upload_file(self, src, dest):
        blob = self.bucket.blob(super()._get_dest(dest))
        blob.upload_from_filename(src)

    def update_version(self, version):
        blob = self.bucket.blob(super()._get_dest('latest_version.txt'))
        blob.upload_from_string(version)


class LocalBucketIO(BucketIO):
    def __init__(self, prefix='', dir_path=''):
        super().__init__(prefix)
        self.dir_path = dir_path

    def upload_file(self, src, dest):
        shutil.copyfile(src, super()._get_dest(dest))

    def update_version(self, version):
        with open(self._get_dest('latest_version.txt'), 'w') as out:
            out.write(version)

    def _get_dest(self, dest):
        return f'{self.dir_path}/{super()._get_dest(dest)}'
