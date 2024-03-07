import json
import logging
import math
import re
from pathlib import Path
from typing import Generator

from API.AbstractRequester import AbstractRequester
from Domain.AssetCollection import AssetCollection
from API.EMInfraRestClient import EMInfraRestClient
from API.EMsonImporter import EMsonImporter
from Domain.Enums import AuthType, Environment, Direction
from Domain.InfoObject import NodeInfoObject
from Exceptions.AssetsMissingError import AssetsMissingError
from Exceptions.ObjectAlreadyExistsError import ObjectAlreadyExistsError
from API.RequesterFactory import RequesterFactory
from pandas import DataFrame, concat


class AssetInfoCollector:
    def __init__(self, settings_path: Path, auth_type: AuthType, env: Environment):
        self.em_infra_importer = EMInfraRestClient(self.create_requester_with_settings(
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

    def _common_collect_relation_info(self, assetrelaties_generator: Generator[dict, None, None],
                                      ignore_duplicates: bool = False) -> None:
        asset_missing_error = AssetsMissingError(msg='')
        for relation in assetrelaties_generator:
            relation['uuid'] = relation.pop('@id')[46:82]
            relation['typeURI'] = relation.pop('@type')
            relation['bron'] = relation['RelatieObject.bron']['@id'][39:75]
            relation['doel'] = relation['RelatieObject.doel']['@id'][39:75]
            try:
                self.collection.add_relation(relation)
            except AssetsMissingError as e:
                asset_missing_error.uuids.extend(e.uuids)
                asset_missing_error.msg += e.msg
            except ObjectAlreadyExistsError as e:
                if not ignore_duplicates:
                    raise e
        if asset_missing_error.uuids:
            raise asset_missing_error

    def collect_relation_info(self, uuids: [str], ignore_duplicates: bool = False) -> None:
        self._common_collect_relation_info(self.get_assetrelaties_by_uuids(uuids=uuids),
                                           ignore_duplicates=ignore_duplicates)

    def collect_relation_info_by_sources_or_targets(self, uuids: [str], ignore_duplicates: bool = False) -> None:
        self._common_collect_relation_info(self.get_assetrelaties_by_source_or_target_uuids(uuids=uuids),
                                           ignore_duplicates=ignore_duplicates)

    def start_collecting_from_starting_uuids(self, starting_uuids: [str]) -> None:
        self.collect_asset_info(uuids=starting_uuids)

        # hardcoded pattern
        # bevestiging verlichtingstoestelLED > console, mast, armatuur
        # console + mast
        # hoort_bij > legacy mast/console

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

    def start_collecting_from_starting_uuids_using_pattern(self, starting_uuids: [str],
                                                           pattern: [tuple[str, str, object]]) -> None:
        uuid_pattern = next((t[2] for t in pattern if t[:2] == ('uuids', 'of')), None)
        type_of_patterns = [t for t in pattern if t[1] == 'type_of']
        relation_patterns = [t for t in pattern if re.match('^(<)?-\\[r(\\d)*]-(>)?$', t[1]) is not None]

        if uuid_pattern is None:
            raise ValueError('No uuids pattern found in pattern list. '
                             'Must contain one tuple with ("uuids", "of", object)')
        if not type_of_patterns:
            raise ValueError('No type_of pattern found in pattern list. '
                             'Must contain at least one tuple with (object, "type_of", object)')
        if not relation_patterns:
            raise ValueError('No relation pattern found in pattern list'
                             'Must contain at least one tuple with (object, "-[r]-", object) where r is followed by a '
                             'number and relation may or may not be directional (using < and > symbols)')

        self.collect_asset_info(uuids=starting_uuids)

        matching_objects = [uuid_pattern]
        while relation_patterns:
            new_matching_objects = []
            for obj in matching_objects:
                relation_patterns = self.order_patterns_for_object(obj, relation_patterns)

                for relation_pattern in relation_patterns:
                    if relation_pattern[0] != obj:
                        continue

                    new_matching_objects.append(relation_pattern[2])

                    type_of_obj = next((t[2] for t in type_of_patterns if t[0] == relation_pattern[0]), None)
                    if type_of_obj is None:
                        raise ValueError(f'No type_of pattern found for object {relation_pattern[0]}')

                    type_of_uuids = [asset.uuid for asset in self.collection.get_node_objects_by_types(type_of_obj)]
                    if not type_of_uuids:
                        continue
                    try:
                        self.collect_relation_info_by_sources_or_targets(uuids=type_of_uuids, ignore_duplicates=True)
                    except AssetsMissingError as e:
                        self.collect_asset_info(uuids=e.uuids)
                        self.collect_relation_info_by_sources_or_targets(uuids=type_of_uuids, ignore_duplicates=True)

                relation_patterns = [t for t in relation_patterns if t[0] != obj]
            matching_objects = new_matching_objects

    def start_creating_report(self, aanlevering_id: str, aanlevering_naam: str) -> DataFrame:
        df = DataFrame()
        all_column_names = [
            'aanlevering_id', 'aanlevering_naam', 'LED_toestel_uuid', 'LED_toestel_naam',
            'LED_toestel_naam_conform_conventie',
            'relatie_naar_armatuur_controller_aanwezig', 'armatuur_controller_uuid', 'armatuur_controller_naam',
            'armatuur_controller_naam_conform_conventie',
            'relatie_naar_drager_aanwezig', 'drager_uuid', 'drager_type', 'drager_naam', 'drager_naam_conform_conventie',
            'relatie_naar_legacy_drager_aanwezig', 'legacy_drager_uuid', 'legacy_drager_type', 'legacy_drager_naampad',
            'legacy_drager_naampad_conform_conventie', 'legacy_drager_LED_toestel_binnen_5_meter']
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
            record_dict = self.get_report_record_for_one_toestel(toestel=toestel, record_dict=current_toestel_dict)
            df_current = DataFrame(record_dict)
            df = concat([df, df_current])

        return df.sort_values('LED_toestel_uuid')

    @classmethod
    def order_patterns_for_object(cls, obj: str, relation_patterns: [tuple[str, str, str]]) -> [tuple[str, str, str]]:
        ordered_patterns = []
        for relation_pattern in relation_patterns:
            if relation_pattern[2] == obj:
                ordered_patterns.append(AssetInfoCollector.reverse_relation_pattern(relation_pattern))
            else:
                ordered_patterns.append(relation_pattern)
        return ordered_patterns

    @classmethod
    def reverse_relation_pattern(cls, relation_pattern: tuple[str, str, str]) -> tuple[str, str, str]:
        rel_str = relation_pattern[1]
        parts = re.match('(<?-)\\[(r.+)](->?)', rel_str).groups()
        parts_2 = parts[0].replace('<', '>')[::-1]
        parts_0 = parts[2].replace('>', '<')[::-1]

        return relation_pattern[2], f'{parts_0}[{parts[1]}]{parts_2}', relation_pattern[0]

    def print_feed_page(self):
        self.em_infra_importer.print_feed_page()

    def get_report_record_for_one_toestel(self, toestel: NodeInfoObject, record_dict: dict) -> dict:
        toestel_uuid = toestel.uuid
        toestel_name = toestel.attr_dict.get('AIMNaamObject.naam', '')

        record_dict['LED_toestel_naam_conform_conventie'] = self.is_conform_name_convention_toestel(
            toestel_name=toestel_name)
        installatie_nummer = self.get_installatie_nummer_from_name(toestel_name=toestel_name)
        lichtpunt_nummer = self.get_lichtpunt_nummer_from_name(toestel_name=toestel_name)

        controllers = list(self.collection.traverse_graph(
            start_uuid=toestel_uuid, relation_types=['Bevestiging'], allowed_directions=[Direction.NONE],
            return_type='info_object', filtered_node_types=['onderdeel#Armatuurcontroller']))

        if not controllers:
            if toestel_name == '':
                toestel_name = toestel_uuid
            logging.info(f"toestel {toestel_name} heeft geen relatie naar een armatuur controller")
            record_dict['relatie_naar_armatuur_controller_aanwezig'] = [False]
        else:
            controller = controllers[0]
            record_dict['relatie_naar_armatuur_controller_aanwezig'] = [True]
            record_dict['armatuur_controller_uuid'] = [controller.uuid]
            controller_name = controller.attr_dict.get('AIMNaamObject.naam', '')
            record_dict['armatuur_controller_naam'] = [controller_name]
            record_dict['armatuur_controller_naam_conform_conventie'] = (
                self.is_conform_name_convention_armatuur_controller(controller_name=controller_name,
                                                                    toestel_name=toestel_name))

        dragers = list(self.collection.traverse_graph(
            start_uuid=toestel_uuid, relation_types=['Bevestiging'], allowed_directions=[Direction.NONE],
            return_type='info_object', filtered_node_types=['onderdeel#WVLichtmast', 'onderdeel#WVConsole',
                                                            'onderdeel#PunctueleVerlichtingsmast']))

        if not dragers:
            if toestel_name == '':
                toestel_name = toestel_uuid
            logging.info(f"toestel {toestel_name} heeft geen relatie naar een drager")
            record_dict['relatie_naar_drager aanwezig'] = [False]
            return record_dict

        drager = dragers[0]
        drager_uuid = drager.uuid
        drager_naam = drager.attr_dict.get('AIMNaamObject.naam', '')
        record_dict['relatie_naar_drager_aanwezig'] = [True]
        record_dict['drager_uuid'] = [drager.uuid]
        record_dict['drager_type'] = [drager.short_type.split('#')[-1]]
        record_dict['drager_naam'] = [drager_naam]
        record_dict['drager_naam_conform_conventie'] = (
            self.is_conform_name_convention_drager(drager_name=drager_naam,
                                                   toestel_name=toestel_name))

        legacy_drager = next(self.collection.traverse_graph(
            start_uuid=drager_uuid, relation_types=['HoortBij'], allowed_directions=[Direction.WITH],
            return_type='info_object', filtered_node_types=['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole',
                                                            'lgc:installatie#VVOP']))

        if legacy_drager is None:
            logging.info(f"drager {drager_uuid} heeft geen relatie naar een legacy equivalent")
            record_dict['relatie_naar_legacy_drager_aanwezig'] = [False]
            return record_dict

        legacy_drager_uuid = legacy_drager.uuid
        legacy_drager_naampad = legacy_drager.attr_dict.get('NaampadObject.naampad', '')
        record_dict['relatie_naar_legacy_drager_aanwezig'] = [True]
        record_dict['legacy_drager_uuid'] = [legacy_drager_uuid]
        record_dict['legacy_drager_type'] = [legacy_drager.short_type.split('#')[-1]]
        record_dict['legacy_drager_naampad'] = [legacy_drager_naampad]
        record_dict['legacy_drager_naampad_conform_conventie'] = (
            self.is_conform_name_convention_legacy_drager(
                legacy_drager_naampad=legacy_drager_naampad, installatie_nummer=installatie_nummer,
                lichtpunt_nummer=lichtpunt_nummer))
        record_dict['legacy_drager_LED_toestel_binnen_5_meter'] = self.is_drager_within_small_radius_legacy_drager(
            legacy_drager=legacy_drager, drager=drager)


        return record_dict

    @classmethod
    def is_conform_name_convention_toestel(cls, toestel_name: str) -> bool:
        parts = toestel_name.split('.')
        if len(parts) != 3:
            return False

        if len(parts[1]) == 0:
            return False

        if not parts[2].startswith('WV'):
            return False

        if not re.match('^(A|C|G|WO|WW)[0-9]{4}$', parts[0]):
            return False

        return True

    @classmethod
    def get_installatie_nummer_from_name(cls, toestel_name: str) -> str:
        if toestel_name is None or not toestel_name or '.' not in toestel_name:
            return ''
        return toestel_name.split('.')[0]

    @classmethod
    def get_lichtpunt_nummer_from_name(cls, toestel_name: str) -> str:
        if toestel_name is None or not toestel_name or '.' not in toestel_name:
            return ''
        return toestel_name.split('.')[1]

    @classmethod
    def is_conform_name_convention_armatuur_controller(cls, controller_name: str, toestel_name: str) -> bool:
        if controller_name is None or not controller_name:
            return False
        if toestel_name is None or not toestel_name:
            return False
        return controller_name.startswith(f'{toestel_name}.AC')

    @classmethod
    def is_conform_name_convention_drager(cls, drager_name: str, toestel_name: str) -> bool:
        if drager_name is None or not drager_name:
            return False
        if toestel_name is None or not toestel_name:
            return False
        return toestel_name.startswith(f'{drager_name}.WV')

    @classmethod
    def is_conform_name_convention_legacy_drager(cls, legacy_drager_naampad: str, installatie_nummer: str,
                                                 lichtpunt_nummer: str) -> bool:
        if legacy_drager_naampad is None or not legacy_drager_naampad:
            return False
        if installatie_nummer is None or not installatie_nummer:
            return False
        if lichtpunt_nummer is None or not lichtpunt_nummer:
            return False
        return legacy_drager_naampad == f'{installatie_nummer}/{installatie_nummer}.WV/{lichtpunt_nummer}'

    @classmethod
    def is_drager_within_small_radius_legacy_drager(cls, legacy_drager: NodeInfoObject, drager: NodeInfoObject):
        if legacy_drager is None or drager is None:
            return False
        legacy_puntlocatie = legacy_drager.attr_dict.get('loc:Locatie.puntlocatie')
        if legacy_puntlocatie is None:
            return False
        legacy_puntgeometrie = legacy_puntlocatie.get('loc:3Dpunt.puntgeometrie')
        if legacy_puntgeometrie is None:
            return False
        legacy_coords = legacy_puntgeometrie.get('loc:DtcCoord.lambert72')
        if legacy_coords is None:
            return False
        legacy_x = legacy_coords.get('loc:DtcCoordLambert72.xcoordinaat')
        legacy_y = legacy_coords.get('loc:DtcCoordLambert72.ycoordinaat')
        if legacy_x is None or legacy_y is None:
            return False

        drager_logs = drager.attr_dict.get('geo:Geometrie.log')
        if drager_logs is None:
            return False
        if len(drager_logs) == 0:
            return False
        log = next((log for log in drager_logs
                    if log.get('geo:DtcLog.niveau') == 'https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/0'),
                   None)
        if log is None:
            log = next((log for log in drager_logs
                        if log.get('geo:DtcLog.niveau') == 'https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/-1'),
                       None)
        if log is None:
            return False
        drager_geometrie = log.get('geo:DtcLog.geometrie')
        if drager_geometrie is None:
            return False
        drager_puntgeometrie = drager_geometrie.get('geo:DtuGeometrie.punt')
        if drager_puntgeometrie is None:
            return False
        # use regex to get coordinates out of wkt string in drager_puntgeometrie
        drager_coords = re.match(r'POINT Z \((\d+.\d+) (\d+.\d+) (\d+)\)', drager_puntgeometrie)
        if len(drager_coords.groups()) != 3:
            return False
        drager_x = float(drager_coords[1])
        drager_y = float(drager_coords[2])

        return math.sqrt(abs(legacy_x - drager_x)**2 + abs(legacy_y - drager_y)**2) < 5
