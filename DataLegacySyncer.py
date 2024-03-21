import json
from itertools import batched
from pathlib import Path

from API.AbstractRequester import AbstractRequester
from API.DavieRestClient import DavieRestClient
from API.EMInfraRestClient import EMInfraRestClient
from API.EMsonImporter import EMsonImporter
from API.RequesterFactory import RequesterFactory
from Database.DbManager import DbManager
from Domain.AssetInfoCollector import AssetInfoCollector
from Domain.DeliveryFinder import DeliveryFinder
from Domain.Enums import AuthType, Environment
from Domain.ReportCreator import ReportCreator


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

    def collect_and_create_reports(self):
        asset_info_collector = AssetInfoCollector(em_infra_rest_client=self.em_infra_client,
                                                  emson_importer=self.emson_importer)
        self._collect_all_info(asset_info_collector=asset_info_collector)
        self._create_all_reports(asset_info_collector=asset_info_collector)

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
    
    def _collect_all_info(self, asset_info_collector, batch_size: int = 10000):
        # work in batches of <batch_size> asset_uuids

        asset_uuids = self.db_manager.get_asset_uuids_from_final_deliveries()[:100]
        for uuids in batched(asset_uuids, batch_size):
            print('collecting asset info')
            asset_info_collector.start_collecting_from_starting_uuids_using_pattern(
                starting_uuids=uuids,
                pattern=[('uuids', 'of', 'a'),
                         ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                         ('a', '-[r1]-', 'b'),
                         ('b', 'type_of',
                          ['onderdeel#WVLichtmast', 'onderdeel#WVConsole', 'onderdeel#Armatuurcontroller']),
                         ('b', '-[r2]->', 'c'),
                         ('a', '-[r2]->', 'c'),
                         ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole',
                                           'lgc:installatie#VPBevestig']),
                         ('r1', 'type_of', ['onderdeel#Bevestiging']),
                         ('r2', 'type_of', ['onderdeel#HoortBij'])])
            print('collected asset info starting from onderdeel#VerlichtingstoestelLED')

        for uuids in batched(asset_uuids, batch_size):
            print('collecting asset info')
            asset_info_collector.start_collecting_from_starting_uuids_using_pattern(
                starting_uuids=uuids,
                pattern=[('uuids', 'of', 'a'),
                         ('a', 'type_of', ['onderdeel#Armatuurcontroller']),
                         ('a', '-[r1]-', 'b'),
                         ('b', 'type_of', ['onderdeel#VerlichtingstoestelLED'])])
            print('collected asset info starting from Armatuurcontroller')

        for uuids in batched(asset_uuids, batch_size):
            print('collecting asset info')
            asset_info_collector.start_collecting_from_starting_uuids_using_pattern(
                starting_uuids=uuids,
                pattern=[('uuids', 'of', 'a'),
                         ('a', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
                         ('a', '-[r1]-', 'b'),
                         ('b', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                         ('a', '-[r2]->', 'c'),
                         ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole',
                                           'lgc:installatie#VPBevestig']),
                         ('r1', 'type_of', ['onderdeel#Bevestiging']),
                         ('r2', 'type_of', ['onderdeel#HoortBij'])])
            print('collected asset info starting from OTL drager')

        for uuids in batched(asset_uuids, batch_size):
            print('collecting asset info')
            asset_info_collector.start_collecting_from_starting_uuids_using_pattern(
                starting_uuids=uuids,
                pattern=[('uuids', 'of', 'a'),
                         ('a', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole',
                                           'lgc:installatie#VPBevestig']),
                         ('a', '<-[r1]-', 'b'),
                         ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole', 
                                           'onderdeel#VerlichtingstoestelLED']),
                         ('r1', 'type_of', ['onderdeel#HoortBij'])])
            print('collected asset info starting from legacy drager')

    def _create_all_reports(self, asset_info_collector):
        report_creator = ReportCreator(collection=asset_info_collector.collection, db_manager=self.db_manager)
        report_creator.create_all_reports()

