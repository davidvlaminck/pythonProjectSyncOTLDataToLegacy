import json
from pathlib import Path

from AbstractRequester import AbstractRequester
from EMInfraImporter import EMInfraImporter
from EMsonImporter import EMsonImporter
from Enums import AuthType, Environment
from RequesterFactory import RequesterFactory


class AssetInfoCollector:
    def __init__(self, settings_path: Path, auth_type: AuthType, env: Environment):
        self.em_infra_importer = EMInfraImporter(self.create_requester_with_settings(
            settings_path=settings_path, auth_type=auth_type, env=env))
        self.emson_importer = EMsonImporter(self.create_requester_with_settings(
            settings_path=settings_path, auth_type=auth_type, env=env))

    @classmethod
    def create_requester_with_settings(cls, settings_path: Path, auth_type: AuthType, env: Environment
                                       ) -> AbstractRequester:
        with open(settings_path) as settings_file:
            settings = json.load(settings_file)
        return RequesterFactory.create_requester(settings=settings, auth_type=auth_type, env=env)

    def get_assets_by_uuids(self, uuids: [str]):
        #return self.em_infra_importer.get_objects_from_oslo_search_endpoint_using_iterator(resource='assets',
         #                                                                                  filter_dict={'uuid': uuids})
        return self.emson_importer.get_assets_by_uuid_using_iterator(uuids=uuids)