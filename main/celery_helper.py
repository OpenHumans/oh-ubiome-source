import os
import logging

logger = logging.getLogger(__name__)


def temp_join(tmp_directory, path):
    return os.path.join(tmp_directory, path)


def filter_archive(zip_file):
    return [f for f in zip_file.namelist()
            if not f.startswith('__MACOSX/')]
