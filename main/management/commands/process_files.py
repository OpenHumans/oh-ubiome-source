from django.core.management.base import BaseCommand
from open_humans.models import OpenHumansMember
from main.celery import clean_uploaded_file
from ohapi import api
from project_admin.models import ProjectConfiguration


class Command(BaseCommand):
    help = 'Requeue all unprocessed files for Celery'

    def iterate_member_files(self, ohmember):
        client_info = ProjectConfiguration.objects.get(id=1).client_info
        ohmember_data = api.exchange_oauth2_member(
                                ohmember.get_access_token(**client_info))
        files = ohmember_data['data']
        for f in files:
            fname = f['basename']
            if not fname.endswith('.zip') and not fname.endswith('.json'):
                api.delete_file(ohmember.access_token,
                                ohmember.oh_id,
                                file_id=f['id'])

    def handle(self, *args, **options):
        open_humans_members = OpenHumansMember.objects.all()
        for ohmember in open_humans_members:
            self.iterate_member_files(ohmember)
