import json
import logging
import math
from pathlib import Path
from typing import Generator

from AbstractRequester import AbstractRequester
from AssetCollection import AssetCollection
from EMInfraImporter import EMInfraImporter
from EMsonImporter import EMsonImporter
from Enums import AuthType, Environment, Direction
from Exceptions.AssetsMissingError import AssetsMissingError
from Exceptions.ObjectAlreadyExistsError import ObjectAlreadyExistsError
from RequesterFactory import RequesterFactory
from pandas import DataFrame, concat


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

    def collect_relation_info(self, uuids: [str]) -> None:
        for asset in self.get_assetrelaties_by_uuids(uuids=uuids):
            asset['uuid'] = asset.pop('@id')[46:82]
            asset['typeURI'] = asset.pop('@type')
            asset['bron'] = asset['RelatieObject.bron']['@id'][39:75]
            asset['doel'] = asset['RelatieObject.doel']['@id'][39:75]
            self.collection.add_relation(asset)

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

    def start_collecting_from_starting_uuids(self, starting_uuids: [str]) -> None:
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

        dragers = self.collection.get_node_objects_by_types(['onderdeel#WVLichtmast', 'onderdeel#WVConsole',
                                                             'onderdeel#PunctueleVerlichtingsmast'])
        drager_uuids = [drager.uuid for drager in dragers]
        try:
            self.collect_relation_info_by_sources_or_targets(uuids=drager_uuids, ignore_duplicates=True)
        except AssetsMissingError as e:
            self.collect_asset_info(uuids=e.uuids)
            self.collect_relation_info_by_sources_or_targets(uuids=drager_uuids, ignore_duplicates=True)

    def start_creating_report(self, aanlevering_id: str, aanlevering_naam: str) -> DataFrame:
        df = DataFrame()
        all_column_names = [
            'aanlevering_id', 'aanlevering_naam', 'LED_toestel_uuid', 'LED_toestel_naam',
            'relatie_naar_armatuur_controller_aanwezig', 'armatuur_controller_uuid', 'armatuur_controller_naam',
            'relatie_naar_drager_aanwezig', 'drager_uuid', 'drager_type', 'drager_naam',
            'relatie_naar_legacy_drager_aanwezig', 'legacy_drager_uuid', 'legacy_drager_type', 'legacy_drager_naampad']
        for missing_column_name in all_column_names:
            df[missing_column_name] = None

        # get all verlichtingstoestelLED
        toestellen = self.collection.get_node_objects_by_types(['onderdeel#VerlichtingstoestelLED'])

        # get mast/console
        for toestel in toestellen:
            toestel_uuid = toestel.uuid
            toestel_name = toestel.attr_dict.get('AIMNaamObject.naam', '')
            current_toestel_dict = {'aanlevering_id': [aanlevering_id], 'aanlevering_naam': [aanlevering_naam],
                                    'LED_toestel_uuid': [toestel_uuid], 'LED_toestel_naam': [toestel_name]}

            if toestel_name is None:
                toestel_name = toestel_uuid

            controllers = list(self.collection.traverse_graph(
                start_uuid=toestel_uuid, relation_types=['Bevestiging'], allowed_directions=[Direction.NONE],
                return_type='info_object', filtered_node_types=['onderdeel#Armatuurcontroller']))

            if not controllers:
                logging.info(f"toestel {toestel_name} heeft geen relatie naar een armatuur controller")
                current_toestel_dict['relatie_naar_armatuur_controller_aanwezig'] = [False]
            else:
                controller = controllers[0]
                current_toestel_dict['relatie_naar_armatuur_controller_aanwezig'] = [True]
                current_toestel_dict['armatuur_controller_uuid'] = [controller.uuid]
                current_toestel_dict['armatuur_controller_naam'] = [controller.attr_dict.get('AIMNaamObject.naam', '')]

            dragers = list(self.collection.traverse_graph(
                start_uuid=toestel_uuid, relation_types=['Bevestiging'], allowed_directions=[Direction.NONE],
                return_type='info_object', filtered_node_types=['onderdeel#WVLichtmast', 'onderdeel#WVConsole',
                                                                'onderdeel#PunctueleVerlichtingsmast']))

            if not dragers:
                logging.info(f"toestel {toestel_name} heeft geen relatie naar een drager")
                current_toestel_dict['relatie_naar_drager aanwezig'] = [False]
                df_current = DataFrame(current_toestel_dict)
                df = concat([df, df_current])
                continue

            drager = dragers[0]
            drager_uuid = drager.uuid
            current_toestel_dict['relatie_naar_drager_aanwezig'] = [True]
            current_toestel_dict['drager_uuid'] = [drager.uuid]
            current_toestel_dict['drager_type'] = [drager.short_type.split('#')[-1]]
            current_toestel_dict['drager_naam'] = [drager.attr_dict.get('AIMNaamObject.naam', '')]

            legacy_drager = next(self.collection.traverse_graph(
                start_uuid=drager_uuid, relation_types=['HoortBij'], allowed_directions=[Direction.WITH],
                return_type='info_object', filtered_node_types=['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole',
                                                                'lgc:installatie#VVOP']))

            if legacy_drager is None:
                logging.info(f"drager {drager_uuid} heeft geen relatie naar een legacy equivalent")
                current_toestel_dict['hoortbij_drager_naar_lgc_aanwezig'] = [False]
                df_current = DataFrame(current_toestel_dict)
                df = concat([df, df_current])
                continue

            legacy_drager_uuid = legacy_drager.uuid
            current_toestel_dict['relatie_naar_legacy_drager_aanwezig'] = [True]
            current_toestel_dict['legacy_drager_uuid'] = [legacy_drager.uuid]
            current_toestel_dict['legacy_drager_type'] = [legacy_drager.short_type.split('#')[-1]]
            current_toestel_dict['legacy_drager_naampad'] = [legacy_drager.attr_dict.get('NaampadObject.naampad', '')]

            df_current = DataFrame(current_toestel_dict)
            df = concat([df, df_current])

        return df.sort_values('LED_toestel_uuid')
