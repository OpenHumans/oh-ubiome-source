from django.conf import settings
import os
from celery import Celery
import tempfile
import json
from ohapi import api
import requests
import logging
from .celery_helper import temp_join, filter_archive
import zipfile

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'oh_data_uploader.settings')
OH_BASE_URL = settings.OPENHUMANS_OH_BASE_URL

logger = logging.getLogger(__name__)

app = Celery('proj')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')
app.conf.update(CELERY_BROKER_URL=os.environ['REDIS_URL'],
                CELERY_RESULT_BACKEND=os.environ['REDIS_URL'])

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()
# app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


def create_tempfile(dfile, suffix):
    tf_in = tempfile.NamedTemporaryFile(suffix="." + suffix)
    tf_in.write(requests.get(dfile['download_url']).content)
    tf_in.flush()
    return tf_in


def verify_ubiome(dfile):
    """
    Verify that this is a VCF file.
    """
    base_name = dfile['basename']
    if base_name.endswith('.zip'):
        input_file = create_tempfile(dfile, ".zip")
        zip_file = zipfile.ZipFile(input_file)
        zip_files = filter_archive(zip_file)
        for filename in zip_files:
            if not filename.endswith('.fastq.gz'):
                raise ValueError(
                    'Found a filename that did not end with ".fastq.gz": '
                    '"{}"'.format(filename))
    else:
        raise ValueError("Input filename doesn't match .vcf, .vcf.gz, "
                         'or .vcf.bz2')


def process_file(dfile, access_token, member, metadata, taxonomy):
    try:
        verify_ubiome(dfile)
        tmp_directory = tempfile.mkdtemp()
        base_filename = dfile['basename'].replace('.zip', '')
        taxonomy_file = base_filename + '.taxonomy.json'
        raw_filename = temp_join(tmp_directory, taxonomy_file)
        metadata = {
                    'description':
                    'uBiome 16S taxonomy data, JSON format.',
                    'tags': ['json', 'uBiome', '16S']
                    }
        with open(raw_filename, 'w') as raw_file:
            json.dump(taxonomy, raw_file)
            raw_file.flush()

        api.upload_aws(raw_filename, metadata,
                       access_token, base_url=OH_BASE_URL,
                       project_member_id=str(member['project_member_id']))
    except:
        api.message("uBiome integration: A broken file was deleted",
                    "While processing your uBiome file "
                    "we noticed that your file does not conform "
                    "to the expected specifications and it was "
                    "thus deleted. Email us as support@openhumans.org if "
                    "you think this file should be valid.",
                    access_token, base_url=OH_BASE_URL)
        api.delete_file(access_token,
                        str(member['project_member_id']),
                        file_id=str(dfile['id']),
                        base_url=OH_BASE_URL)
        raise


@app.task(bind=True)
def clean_uploaded_file(self, access_token, file_id, taxonomy):
    member = api.exchange_oauth2_member(access_token, base_url=OH_BASE_URL)
    for dfile in member['data']:
        if dfile['id'] == file_id:
            process_file(dfile,
                         access_token,
                         member,
                         dfile['metadata'],
                         taxonomy)
    pass
