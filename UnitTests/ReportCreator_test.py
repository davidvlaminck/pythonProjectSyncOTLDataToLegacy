from copy import deepcopy
from unittest.mock import Mock

import pytest

from API.AbstractRequester import AbstractRequester
from Database.DatabaseModel import Delivery
from Database.DbManager import DbManager
from Domain.AssetInfoCollector import AssetInfoCollector
from Domain.InfoObject import NodeInfoObject
from Domain.ReportCreator import ReportCreator
from UnitTests.FakeEminfraImporter import fake_em_infra_importer


def fake_get_deliveries_by_asset_uuid(asset_uuid: str) -> [Delivery]:
    return [Delivery(referentie='DA-01', uuid_davie='01')]


fake_db_manager = Mock(spec=DbManager)
fake_db_manager.get_deliveries_by_asset_uuid = fake_get_deliveries_by_asset_uuid


def test_start_creating_report():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_rest_client=fake_em_infra_importer, emson_importer=Mock())
    report_creator = ReportCreator(collection=collector.collection, db_manager=fake_db_manager)

    collector.collect_asset_info(
        uuids=['00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003',
               '00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000005',
               '00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000007',
               '00000000-0000-0000-0000-000000000008', '00000000-0000-0000-0000-000000000009',
               '00000000-0000-0000-0000-000000000026', '00000000-0000-0000-0000-000000000022',
               '00000000-0000-0000-0000-000000000023', '00000000-0000-0000-0000-000000000024'])
    collector.collect_relation_info(
        uuids=['000000000002-Bevestigin-000000000004', '000000000006-Bevestigin-000000000002',
               '000000000005-Bevestigin-000000000003', '000000000003-Bevestigin-000000000007',
               '000000000004--HoortBij--000000000008', '000000000005--HoortBij--000000000009',
               '000000000024-Bevestigin-000000000004', '000000000023-Bevestigin-000000000004',
               '000000000022-Bevestigin-000000000004', '000000000002-Bevestigin-000000000026'])

    expected_report = {
        'columns': [
            'aanlevering_id', 'aanlevering_naam', 'legacy_drager_uuid', 'legacy_drager_type',
            'legacy_drager_naampad', 'legacy_drager_naampad_conform_conventie',
            'drager_verwacht', 'relatie_legacy_naar_drager_aanwezig',
            'drager_uuid', 'drager_type', 'drager_naam', 'drager_naam_conform_conventie',
            'relatie_drager_naar_toestel_aanwezig',
            'LED_toestel_1_uuid', 'LED_toestel_1_naam', 'LED_toestel_1_naam_conform_conventie',
            'relatie_naar_armatuur_controller_1_aanwezig',
            'armatuur_controller_1_uuid', 'armatuur_controller_1_naam', 'armatuur_controller_1_naam_conform_conventie',
            'legacy_drager_en_drager_binnen_5_meter',
            'legacy_drager_en_drager_identieke_geometrie', 'update_legacy_drager_geometrie',
            'legacy_drager_en_drager_gelijke_toestand', 'update_legacy_drager_toestand',
            'attributen_gelijk', 'update_legacy_drager_attributen'],
        'index': [0, 0],
        'data': [
            ['01', 'DA-01', '00000000-0000-0000-0000-000000000008', 'VPLMast',
             'A0000/A0000.WV/A01', True,  # 'legacy_drager_naampad', 'legacy_drager_naampad_conform_conventie',
             True, True,  # 'drager_verwacht', 'relatie_legacy_naar_drager_aanwezig',
             # 'drager_uuid', 'drager_type', 'drager_naam', 'drager_naam_conform_conventie',
             '00000000-0000-0000-0000-000000000004', 'WVLichtmast', 'A0000.A01', True,
             True,  # 'relatie_drager_naar_toestel_aanwezig',
             # 'LED_toestel_1_uuid', 'LED_toestel_1_naam', 'LED_toestel_1_naam_conform_conventie',
             '00000000-0000-0000-0000-000000000002', 'A0000.A01.WV1', True,
             True,  # 'relatie_naar_armatuur_controller_1_aanwezig',
             # 'armatuur_controller_1_uuid', 'armatuur_controller_1_naam', 'armatuur_controller_1_naam_conform_conventie'
             '00000000-0000-0000-0000-000000000006', 'A0000.A01.WV1.AC1', True,
             True,
             False, '',
             True, '',
             True, ''],
            ['01', 'DA-01', '00000000-0000-0000-0000-000000000009', 'VPConsole',
             'A0000/A0000.WV/C02', True,  # 'legacy_drager_naampad', 'legacy_drager_naampad_conform_conventie',
             True, True,  # 'drager_verwacht', 'relatie_legacy_naar_drager_aanwezig',
             # 'drager_uuid', 'drager_type', 'drager_naam', 'drager_naam_conform_conventie',
             '00000000-0000-0000-0000-000000000005', 'WVConsole', 'A0000.FOUT1', False,
             True,  # 'relatie_drager_naar_toestel_aanwezig',
             # 'LED_toestel_1_uuid', 'LED_toestel_1_naam', 'LED_toestel_1_naam_conform_conventie',
             '00000000-0000-0000-0000-000000000003', 'A0000.C02.WV1', True,
             True,  # 'relatie_naar_armatuur_controller_1_aanwezig',
             # 'armatuur_controller_1_uuid', 'armatuur_controller_1_naam', 'armatuur_controller_1_naam_conform_conventie'
             '00000000-0000-0000-0000-000000000007', 'A0000.C02.WV1.AC1', True,
             False,
             False,
             '',
             False,
             'uit-gebruik',
             False,
             '{\n'
             '    "aantal_te_verlichten_rijvakken_LED": null,\n'
             '    "datum_installatie_LED": null,\n'
             '    "kleurtemperatuur_LED": null,\n'
             '    "lichtpunthoogte_tov_rijweg": null,\n'
             '    "lumen_pakket_LED": null,\n'
             '    "overhang_LED": null,\n'
             '    "verlichtingsniveau_LED": null,\n'
             '    "verlichtingstoestel_merk_en_type": null,\n'
             '    "verlichtingstoestel_systeemvermogen": null,\n'
             '    "verlichtingstype": null\n'
             '}']]}

    report = report_creator.start_creating_report_pov_legacy()
    report = report.iloc[:, :-21]

    assert report.to_dict('split') == expected_report


def test_is_conform_name_convention_toestel():
    assert ReportCreator.is_conform_name_convention_toestel(
        toestel_name='A0000.A01.WV1', installatie_nummer='A0000', lichtpunt_nummer='A01', toestel_index=1)
    assert ReportCreator.is_conform_name_convention_toestel(
        toestel_name='A0002.A02.WV2', installatie_nummer='A0002', lichtpunt_nummer='A02', toestel_index=2)
    assert not ReportCreator.is_conform_name_convention_toestel(
        toestel_name='A0000.A01.WV1', installatie_nummer='A0000', lichtpunt_nummer='A01', toestel_index=2)
    assert not ReportCreator.is_conform_name_convention_toestel(
        toestel_name='A0000.A01.WV2', installatie_nummer='A0000', lichtpunt_nummer='A02', toestel_index=1)
    assert not ReportCreator.is_conform_name_convention_toestel(
        toestel_name='A0001.A01.WV1', installatie_nummer='A0000', lichtpunt_nummer='A01', toestel_index=1)
    assert not ReportCreator.is_conform_name_convention_toestel(
        toestel_name='A0000.A01.WV1.AC1', installatie_nummer='A0000', lichtpunt_nummer='A01', toestel_index=1)
    assert not ReportCreator.is_conform_name_convention_toestel(
        toestel_name='A0000.A01.WV', installatie_nummer='A0000', lichtpunt_nummer='A01', toestel_index=1)


def test_is_conform_name_convention_armatuur_controller():
    assert ReportCreator.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV1.AC1', toestel_name='A0000.A01.WV1')
    assert ReportCreator.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV2.AC1', toestel_name='A0000.A01.WV2')
    assert not ReportCreator.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.AC1', toestel_name='A0000.A01.WV1')
    assert not ReportCreator.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV1', toestel_name='A0000.A01.WV1.AC1')
    assert not ReportCreator.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV2.AC1', toestel_name='A0000.A01.WV1')


def test_is_conform_name_convention_drager():
    assert ReportCreator.is_conform_name_convention_drager(
        drager_name='A0000.A01', installatie_nummer='A0000', lichtpunt_nummer='A01')
    assert ReportCreator.is_conform_name_convention_drager(
        drager_name='A0001.A02', installatie_nummer='A0001', lichtpunt_nummer='A02')
    assert not ReportCreator.is_conform_name_convention_drager(
        drager_name='A0000.A01.WV1', installatie_nummer='A0000', lichtpunt_nummer='A01')
    assert not ReportCreator.is_conform_name_convention_drager(
        drager_name='A0000.A01.WV1.AC1', installatie_nummer='A0000', lichtpunt_nummer='A01')
    assert not ReportCreator.is_conform_name_convention_drager(
        drager_name='A0000.A02', installatie_nummer='A0001', lichtpunt_nummer='A02')


def test_is_conform_name_convention_legacy_drager():
    assert ReportCreator.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0000/A0000.WV/A01', installatie_nummer='A0000', lichtpunt_nummer='A01')
    assert ReportCreator.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0001/A0001.WV/A02', installatie_nummer='A0001', lichtpunt_nummer='A02')
    assert not ReportCreator.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0002/A0002.WV/A02', installatie_nummer='A0001', lichtpunt_nummer='A02')
    assert not ReportCreator.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0001/A0001.WV/A02', installatie_nummer='A0001', lichtpunt_nummer='A01')
    assert not ReportCreator.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A00002/A0002.WV/A02', installatie_nummer='A00002', lichtpunt_nummer='A02')


def test_get_installatie_nummer_from_naampad():
    assert ReportCreator.get_installatie_nummer_from_naampad('A0000/A0000.WV/A01') == 'A0000'
    assert ReportCreator.get_installatie_nummer_from_naampad('0000/A0000.WV/A01') == '0000'
    assert ReportCreator.get_installatie_nummer_from_naampad('A0000/A0000.WV/101') == 'A0000'
    assert ReportCreator.get_installatie_nummer_from_naampad('A0000') == ''
    assert ReportCreator.get_installatie_nummer_from_naampad('') == ''
    assert ReportCreator.get_installatie_nummer_from_naampad(None) == ''


def test_get_installatie_nummer_from_toestel_name():
    assert ReportCreator.get_installatie_nummer_from_toestel_name('A0000.A01.WV1') == 'A0000'
    assert ReportCreator.get_installatie_nummer_from_toestel_name('0000.A01.WV1') == '0000'
    assert ReportCreator.get_installatie_nummer_from_toestel_name('A0000.A01.WV1.AC1') == 'A0000'
    assert ReportCreator.get_installatie_nummer_from_toestel_name('A0000') == ''
    assert ReportCreator.get_installatie_nummer_from_toestel_name('') == ''
    assert ReportCreator.get_installatie_nummer_from_toestel_name(None) == ''


def test_get_lichtpunt_nummer_from_toestel_name():
    assert ReportCreator.get_lichtpunt_nummer_from_toestel_name('A0000.A01.WV1') == 'A01'
    assert ReportCreator.get_lichtpunt_nummer_from_toestel_name('0000.01.WV1') == '01'
    assert ReportCreator.get_lichtpunt_nummer_from_toestel_name('A0000.A01.WV1.AC1') == 'A01'
    assert ReportCreator.get_lichtpunt_nummer_from_toestel_name('A0000') == ''
    assert ReportCreator.get_lichtpunt_nummer_from_toestel_name('') == ''
    assert ReportCreator.get_lichtpunt_nummer_from_toestel_name(None) == ''


@pytest.mark.parametrize("value, expected", [
    ('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedOverhang/1-0', 'O+1'),
    ('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedOverhang/1-0-2', 'O-1'),
    ('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlWvLedOverhang/0-2', '0'),
])
def test_map_overhang(value, expected):
    assert ReportCreator.map_overhang(value) == expected


def test_distance_between_drager_and_legacy_drager():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_rest_client=fake_em_infra_importer, emson_importer=Mock())

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000008',
                                        '00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000009'])

    drager_node_1 = deepcopy(collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000004'))
    legacy_drager_node_1 = deepcopy(
        collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000008'))
    drager_node_2 = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000005')
    legacy_drager_node_2 = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000009')

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=None, legacy_drager=None) == 100.0

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_2, legacy_drager=legacy_drager_node_2) == 100.0

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 1.0

    drager_node_1.attr_dict['geo:Geometrie.log'][0]['geo:DtcLog.geometrie'][
        'geo:DtuGeometrie.punt'] = 'POINT Z (200005.0 200004.0 0)'

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 5.0

    del drager_node_1.attr_dict['geo:Geometrie.log'][0]['geo:DtcLog.geometrie']['geo:DtuGeometrie.punt']

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0

    del drager_node_1.attr_dict['geo:Geometrie.log'][0]['geo:DtcLog.geometrie']

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0

    del drager_node_1.attr_dict['geo:Geometrie.log'][0]

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0

    del drager_node_1.attr_dict['geo:Geometrie.log']

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0

    del legacy_drager_node_1.attr_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie'][
        'loc:DtcCoord.lambert72']['loc:DtcCoordLambert72.xcoordinaat']

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0

    del legacy_drager_node_1.attr_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie']['loc:DtcCoord.lambert72']

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0

    del legacy_drager_node_1.attr_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie']

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0

    del legacy_drager_node_1.attr_dict['loc:Locatie.puntlocatie']

    assert ReportCreator.distance_between_drager_and_legacy_drager(
        drager=drager_node_1, legacy_drager=legacy_drager_node_1) == 100.0


def test_get_attribute_dict_from_legacy_drager():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_rest_client=fake_em_infra_importer, emson_importer=Mock())

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000008', '00000000-0000-0000-0000-000000000009'])
    legacy_drager_node_1 = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000008')
    legacy_drager_node_2 = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000009')

    d_1 = ReportCreator.get_attribute_dict_from_legacy_drager(legacy_drager=legacy_drager_node_1)
    d_2 = ReportCreator.get_attribute_dict_from_legacy_drager(legacy_drager=legacy_drager_node_2)

    assert d_1 == {
        'aantal_te_verlichten_rijvakken_LED': 'R1',
        'aantal_verlichtingstoestellen': 4,
        'contractnummer_levering_LED': '123456',
        'datum_installatie_LED': '2020-01-01',
        'kleurtemperatuur_LED': 'K3000',
        'LED_verlichting': True,
        'drager_buiten_gebruik': False,
        'lichtpunthoogte_tov_rijweg': 6,
        'lumen_pakket_LED': 10000,
        'overhang_LED': 'O+1',
        'RAL_kleur': '7038',
        'serienummer_armatuurcontroller_1': '1234561',
        'serienummer_armatuurcontroller_2': '1234562',
        'serienummer_armatuurcontroller_3': '1234563',
        'serienummer_armatuurcontroller_4': '1234564',
        'verlichtingsniveau_LED': 'M3',
        'verlichtingstoestel_merk_en_type': 'Schreder Ampera',
        'verlichtingstoestel_systeemvermogen': 100,
        'verlichtingstype': 'hoofdbaan'
    }
    assert d_2 == {
        'aantal_te_verlichten_rijvakken_LED': 'R2',
        'aantal_verlichtingstoestellen': 1,
        'contractnummer_levering_LED': '123456',
        'datum_installatie_LED': '2020-01-01',
        'kleurtemperatuur_LED': 'K3000',
        'LED_verlichting': True,
        'drager_buiten_gebruik': False,
        'lichtpunthoogte_tov_rijweg': 5,
        'lumen_pakket_LED': 10000,
        'overhang_LED': '0',
        'RAL_kleur': '7038',
        'serienummer_armatuurcontroller_1': '1234561',
        'verlichtingsniveau_LED': 'M2',
        'verlichtingstoestel_merk_en_type': 'Schreder Ampera',
        'verlichtingstoestel_systeemvermogen': 100,
        'verlichtingstype': 'hoofdbaan'
    }


def test_get_attribute_dict_from_drager():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(em_infra_rest_client=fake_em_infra_importer, emson_importer=Mock())

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000002',
                                        '00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000022',
                                        '00000000-0000-0000-0000-000000000023', '00000000-0000-0000-0000-000000000024',
                                        '00000000-0000-0000-0000-000000000026'])
    drager = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000004')
    toestellen = collector.collection.get_node_objects_by_types(['onderdeel#VerlichtingstoestelLED'])
    armatuur_controllers = collector.collection.get_node_objects_by_types(['onderdeel#Armatuurcontroller'])

    d_1 = ReportCreator.get_attribute_dict_from_otl_assets(drager=drager, toestellen=list(toestellen),
                                                                armatuur_controllers=list(armatuur_controllers))

    d_expected = {
        'contractnummer_levering_LED': '123456',
        'drager_buiten_gebruik': False,
    }

    d_expected = {
        'aantal_armen': '4',
        'aantal_verlichtingstoestellen': 4,
        'aantal_te_verlichten_rijvakken_LED': 'R1',
        'datum_installatie_LED': '2020-01-01',
        'kleurtemperatuur_LED': 'K3000',
        'LED_verlichting': True,
        'lichtpunthoogte_tov_rijweg': 6.0,
        'lumen_pakket_LED': 10000,
        'overhang_LED': 'O+1',
        'RAL_kleur': '7038',
        'serienummer_armatuurcontroller_1': '1234561',
        'serienummer_armatuurcontroller_2': '1234562',
        'verlichtingsniveau_LED': 'M3',
        'verlichtingstoestel_merk_en_type': 'Schreder Ampera',
        'verlichtingstoestel_systeemvermogen': 100,
        'verlichtingstype': 'hoofdbaan'
    }

    assert d_1 == d_expected


def node_info_object_mock(verlichtingstype_value):
    node_info_object = Mock(spec=NodeInfoObject)
    node_info_object.attr_dict = {
        'Verlichtingstoestel.verlichtGebied':
            'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerlichtingstoestelVerlichtGebied/' +
            verlichtingstype_value}
    return node_info_object


def test_get_verlichtingstype():
    toestellen = [node_info_object_mock('hoofdweg'), node_info_object_mock('hoofdweg')]
    assert ReportCreator.get_verlichtingstype(toestellen) == 'hoofdbaan'
    toestellen = [node_info_object_mock('hoofdweg'), node_info_object_mock('fietspad')]
    assert ReportCreator.get_verlichtingstype(toestellen) == 'hoofdbaan'
    toestellen = [node_info_object_mock('fietspad'), node_info_object_mock('fietspad')]
    assert ReportCreator.get_verlichtingstype(toestellen) == 'fietspadverlichting'
    toestellen = [node_info_object_mock('hoofdweg'), node_info_object_mock('fietspad'),
                  node_info_object_mock('hoofdweg'), node_info_object_mock('fietspad')]
    assert ReportCreator.get_verlichtingstype(toestellen) == 'hoofdbaan'
    toestellen = [node_info_object_mock('hoofdweg'), node_info_object_mock('afrit')]
    assert ReportCreator.get_verlichtingstype(toestellen) == 'opafrit'
