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

    def collect_and_create_specific_reports(self, delivery_references: list[str], combine_single_report: bool = False):
        if combine_single_report:
            asset_info_collector = AssetInfoCollector(em_infra_rest_client=self.em_infra_client,
                                                      emson_importer=self.emson_importer)
            asset_uuids = list(self.db_manager.get_asset_uuids_from_specific_deliveries(
                delivery_references=delivery_references))
            self._collect_info_given_asset_uuids(asset_info_collector=asset_info_collector, asset_uuids=asset_uuids)
            self._create_all_reports(asset_info_collector=asset_info_collector)
        else:
            for delivery_reference in delivery_references:
                asset_info_collector = AssetInfoCollector(em_infra_rest_client=self.em_infra_client,
                                                          emson_importer=self.emson_importer)
                asset_uuids = list(self.db_manager.get_asset_uuids_from_specific_deliveries(
                    delivery_references=[delivery_reference]))
                self._collect_info_given_asset_uuids(asset_info_collector=asset_info_collector, asset_uuids=asset_uuids)
                self._create_all_reports(asset_info_collector=asset_info_collector)

    def collect_and_create_reports(self):
        asset_info_collector = AssetInfoCollector(em_infra_rest_client=self.em_infra_client,
                                                  emson_importer=self.emson_importer)
        asset_uuids = self.db_manager.get_asset_uuids_from_final_deliveries()
        self._collect_info_given_asset_uuids(asset_info_collector=asset_info_collector, asset_uuids=asset_uuids)
        self._create_all_reports(asset_info_collector=asset_info_collector)

    def poll_aanleveringen(self):
        pass
        # poll often to check for status 'Goedgekeurd'

    def update_legacy_data(self):
        pass
    # 4) update legacy data
    #     - [ ] find reports in state db that have sufficient information to using for updates
    #     - [ ] update legacy data using the state report until everything is marked as done
    #     - [ ] update the status of the aanlevering in state db to 'verwerkt'
    
    @staticmethod
    def _collect_info_given_asset_uuids(asset_info_collector: AssetInfoCollector, asset_uuids: list[str],
                                        batch_size: int = 10000):
        # work in batches of <batch_size> asset_uuids
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
                         ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
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

    def sync_specific_deliveries(self, context_strings: [str]):
        delivery_finder = DeliveryFinder(em_infra_client=self.em_infra_client, davie_client=self.davie_client,
                                         db_manager=self.db_manager)

        delivery_finder.sync_specific_deliveries(context_strings=context_strings)

    def process_report(self, report_path: Path = Path('Reports/report.xlsx')):
        report_creator = ReportCreator(collection=None, db_manager=self.db_manager)
        report_creator.process_report(report_path=report_path, em_infra_client=self.em_infra_client)

