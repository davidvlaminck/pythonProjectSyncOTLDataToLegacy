import json
from pathlib import Path
from typing import Generator

from AbstractRequester import AbstractRequester
from AssetCollection import AssetCollection
from EMInfraImporter import EMInfraImporter
from EMsonImporter import EMsonImporter
from Enums import AuthType, Environment
from Exceptions.AssetsMissingError import AssetsMissingError
from Exceptions.ObjectAlreadyExistsError import ObjectAlreadyExistsError
from RequesterFactory import RequesterFactory


class AssetInfoCollector:
    def __init__(self, settings_path: Path, auth_type: AuthType, env: Environment):
        self.em_infra_importer = EMInfraImporter(self.create_requester_with_settings(
            settings_path=settings_path, auth_type=auth_type, env=env))
        self.emson_importer = EMsonImporter(self.create_requester_with_settings(
            settings_path=settings_path, auth_type=auth_type, env=env))
        self.collection = AssetCollection()

    @classmethod
    def create_requester_with_settings(cls, settings_path: Path, auth_type: AuthType, env: Environment
                                       ) -> AbstractRequester:
        with open(settings_path) as settings_file:
            settings = json.load(settings_file)
        return RequesterFactory.create_requester(settings=settings, auth_type=auth_type, env=env)

    def get_assets_by_uuids(self, uuids: [str]) -> Generator[dict, None, None]:
        return self.em_infra_importer.get_objects_from_oslo_search_endpoint_using_iterator(resource='assets',
                                                                                           filter_dict={'uuid': uuids})
        # return self.emson_importer.get_assets_by_uuid_using_iterator(uuids=uuids)

    def get_assetrelaties_by_uuids(self, uuids: [str]) -> Generator[dict, None, None]:
        return self.em_infra_importer.get_objects_from_oslo_search_endpoint_using_iterator(resource='assetrelaties',
                                                                                           filter_dict={'uuid': uuids})

    def get_assetrelaties_by_source_or_target_uuids(self, uuids: [str]) -> Generator[dict, None, None]:
        return self.em_infra_importer.get_objects_from_oslo_search_endpoint_using_iterator(resource='assetrelaties',
                                                                                           filter_dict={'asset': uuids})

    def collect_asset_info(self, uuids: [str]) -> None:
        for asset in self.get_assets_by_uuids(uuids=uuids):
            asset['uuid'] = asset.pop('@id')[39:75]
            asset['typeURI'] = asset.pop('@type')
            self.collection.add_node(asset)

    def collect_relation_info_by_sources_or_targets(self, uuids: [str], ignore_duplicates: bool = False) -> None:
        asset_missing_error = AssetsMissingError(msg='')
        for asset in self.get_assetrelaties_by_source_or_target_uuids(uuids=uuids):
            asset['uuid'] = asset.pop('@id')[46:82]
            asset['typeURI'] = asset.pop('@type')
            asset['bron'] = asset['RelatieObject.bron']['@id'][39:75]
            asset['doel'] = asset['RelatieObject.doel']['@id'][39:75]
            try:
                self.collection.add_relation(asset)
            except AssetsMissingError as e:
                asset_missing_error.uuids.extend(e.uuids)
                asset_missing_error.msg += e.msg
            except ObjectAlreadyExistsError as e:
                if not ignore_duplicates:
                    raise e
        if asset_missing_error.uuids:
            raise asset_missing_error

    def start_collecting_from_starting_uuids(self, starting_uuids):
        self.collect_asset_info(uuids=starting_uuids)

        # hardcoded pattern
        # bevestiging verlichtingstoestelLED > console, mast, armatuur
        # console + mast
        # hoortbij > legacy mast/console

        toestellen = self.collection.get_node_objects_by_types(['onderdeel#VerlichtingstoestelLED'])
        toestel_uuids = [toestel.uuid for toestel in toestellen]
        try:
            self.collect_relation_info_by_sources_or_targets(uuids=toestel_uuids, ignore_duplicates=True)
        except AssetsMissingError as e:
            self.collect_asset_info(uuids=e.uuids)
            self.collect_relation_info_by_sources_or_targets(uuids=toestel_uuids, ignore_duplicates=True)

        dragers = self.collection.get_node_objects_by_types(['onderdeel#WVLichtmast', 'onderdeel#WVConsole'])
        drager_uuids = [drager.uuid for drager in dragers]
        try:
            self.collect_relation_info_by_sources_or_targets(uuids=drager_uuids, ignore_duplicates=True)
        except AssetsMissingError as e:
            self.collect_asset_info(uuids=e.uuids)
            self.collect_relation_info_by_sources_or_targets(uuids=drager_uuids, ignore_duplicates=True)

