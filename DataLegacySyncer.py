import json
from pathlib import Path

from API.AbstractRequester import AbstractRequester
from API.DavieRestClient import DavieRestClient
from API.EMInfraRestClient import EMInfraRestClient
from API.EMsonImporter import EMsonImporter
from API.RequesterFactory import RequesterFactory
from Database.DbManager import DbManager
from Domain.DeliveryFinder import DeliveryFinder
from Domain.Enums import AuthType, Environment


class DataLegacySyncer:
    def __init__(self, settings_path: Path, auth_type: AuthType, env: Environment, state_db_path: Path):
        self.em_infra_client = EMInfraRestClient(self.create_requester_with_settings(
            settings_path=settings_path, auth_type=auth_type, env=env))
        self.emson_importer = EMsonImporter(self.create_requester_with_settings(
            settings_path=settings_path, auth_type=auth_type, env=env))
        self.davie_client = DavieRestClient(self.create_requester_with_settings(
            settings_path=settings_path, auth_type=auth_type, env=env))
        self.db_manager = DbManager(state_db_path=state_db_path)

    @classmethod
    def create_requester_with_settings(cls, settings_path: Path, auth_type: AuthType, env: Environment
                                       ) -> AbstractRequester:
        with open(settings_path) as settings_file:
            settings = json.load(settings_file)
        return RequesterFactory.create_requester(settings=settings, auth_type=auth_type, env=env)

    def run(self):
        self.find_deliveries_to_sync()

    def find_deliveries_to_sync(self):
        delivery_finder = DeliveryFinder(em_infra_client=self.em_infra_client, davie_client=self.davie_client,
                                         db_manager=self.db_manager)

        delivery_finder.find_deliveries_to_sync()


    def poll_aanleveringen(self):
        pass
    # poll often to check for status 'Goedgekeurd'

    def use_aanlevering_to_generate_report(self):
        pass
    # 3) use validated aanlevering
    #     - [ ] find all assets that were changed in that aanlevering (wait for EM-3200 or using emson endpoint)
    #     - [x] collect all related asset information using the asset info collector algorithm
    #     - [/] generate a report to update legacy data with
    #     - [ ] save the report in the state db

    def update_legacy_data(self):
        pass
    # 4) update legacy data
    #     - [ ] find reports in state db that have sufficient information to using for updates
    #     - [ ] update legacy data using the state report until everything is marked as done
    #     - [ ] update the status of the aanlevering in state db to 'verwerkt'
