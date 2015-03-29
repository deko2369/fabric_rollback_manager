# -*- coding: utf-8 -*-
import os

from datetime import datetime

from fabric.api import run, local, cd
from fabric.contrib.files import exists

class RollbackManager:
    def __init__(self, remote_filepath):
        (self.remote_basepath, self.remote_filename) = \
                os.path.split(remote_filepath)

        self.archive_filename = self.remote_filename + '.tar.gz'

    def commit(self, remove=False):
        with cd(self.remote_basepath):
            date_str = datetime.now().strftime('%Y%m%d%H%M%S')

            if not exists(self.archive_filename):
                # create new tar file, and compress file
                tar_args = (self.remote_filename,
                            date_str,
                            self.archive_filename,
                            self.remote_filename)
                run('tar --transform="s/%s/%s/" -czf "%s" "%s"' % tar_args)
            else:
                # append file to already exists tar file and compress
                temp_filename = run('mktemp /tmp/%s.XXXXXXXX' % self.archive_filename)
                try:
                    run('zcat "{0}" > "{1}" && '
                        'tar -rf "{1}" --transform="s/{2}/{3}/" "{2}" && '
                        'gzip -c "{1}" > "{0}"'.format(
                        self.archive_filename, temp_filename,
                        self.remote_filename, date_str)
                    )
                finally:
                    run('rm -f "%s"' % temp_filename)

            # TODO: use --remove-files parameter by tar
            if remove:
                # remove commited file at last
                run('rm -f "%s"' % self.remote_filename)

    def rollback(self, revision=0):
        if not exists(self.archive_filename):
            raise IOError('File not found: %s' % self.archive_filename)

        with cd(self.remote_basepath):
            # get extracted file list
            out = run('tar -tzf "%s"' % self.archive_filename)
            filenames = list(reversed(out.splitlines()))

            if not filenames or len(filenames) <= revision:
                raise RuntimeError('No compressed file or invalid revision')

            # get filename to extract for rollback
            compressed_filename = filenames[revision]

            # extract
            run('tar --transform="s/{0}/{1}/" -xzf "{2}" "{0}"'.format(
                compressed_filename,
                self.remote_filename,
                self.archive_filename)
            )

            # update archive
            temp_filename = run('mktemp /tmp/%s.XXXXXXXX' % self.archive_filename)
            try:
                run('zcat "{0}" | '
                    'tar --delete "{2}" | '
                    'gzip > "{1}" && '
                    'mv "{1}" "{0}"'.format(
                    self.archive_filename, temp_filename, compressed_filename)
                )
            finally:
                run('rm -f "%s"' % temp_filename)

    def count(self):
        if not exists(self.archive_filename):
            return 0

        return int(run('tar -tf "%s" | wc -l' % self.archive_filename))

