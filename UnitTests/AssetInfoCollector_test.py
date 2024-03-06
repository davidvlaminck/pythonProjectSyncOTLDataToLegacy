import logging
import math
from unittest.mock import Mock

from API.AbstractRequester import AbstractRequester
from Domain.AssetInfoCollector import AssetInfoCollector
from API.EMInfraRestClient import EMInfraRestClient


def fake_get_objects_from_oslo_search_endpoint_using_iterator(resource: str, cursor: str | None = None,
                                                              size: int = 100, filter_dict: dict = None):
    asset_1 = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000001-bGdjOmluc3RhbGxhdGllI0thc3Q",
        "AIMDBStatus.isActief": True,
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000001-bGdjOmluc3RhbGxhdGllI0thc3Q"
        },
        "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "tz:Schadebeheerder.schadebeheerder": {
            "tz:DtcBeheerder.naam": "District Schadebeheerder",
            "tz:DtcBeheerder.referentie": "100"
        },
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
        "AIMObject.notitie": "",
        "NaampadObject.naampad": "NAAMPAD/KAST",
        "AIMNaamObject.naam": "KAST",
        "loc:Locatie.omschrijving": "omschrijving",
    }
    asset_inactief = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000010-bGdjOmluc3RhbGxhdGllI0thc3Q",
        "AIMDBStatus.isActief": False
    }
    asset_2 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000002-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000002-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "geo:Geometrie.log": [
            {
                "geo:DtcLog.bron": "https://geo.data.wegenenverkeer.be/id/concept/KlLogBron/overerving",
                "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/0",
                "geo:DtcLog.geometrie": {
                    "geo:DtuGeometrie.punt": "POINT Z (200000.00 200000.00 0)"
                },
            }
        ],
    }
    asset_3 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000003-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.C02.WV1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000003-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
    }
    asset_4 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000004-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
        "geo:Geometrie.log": [
            {
                "geo:DtcLog.bron": "https://geo.data.wegenenverkeer.be/id/concept/KlLogBron/meettoestel",
                "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/0",
                "geo:DtcLog.geometrie": {
                    "geo:DtuGeometrie.punt": "POINT Z (200000.00 200000.00 0)"
                },
            }
        ],
    }
    asset_5 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000005-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.FOUT1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000005-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
    }
    asset_6 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000006-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.A01.WV1.AC1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000006-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
    }
    asset_7 = {
        "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000007-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A0000.C02.WV1.AC1',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000007-"
        },
        "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
    }
    asset_8 = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000008-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'A01',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000008-"
        },
        "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
        "NaampadObject.naampad": "A0000/A0000.WV/A01",
        "loc:Locatie.puntlocatie": {
            "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/manueel",
            "loc:3Dpunt.puntgeometrie": {
                "loc:DtcCoord.lambert72": {
                    "loc:DtcCoordLambert72.ycoordinaat": 200001.0,
                    "loc:DtcCoordLambert72.zcoordinaat": 0,
                    "loc:DtcCoordLambert72.xcoordinaat": 200001.0
                }
            },
            "loc:DtcPuntlocatie.precisie": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/meter"
        }
    }
    asset_9 = {
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPConsole",
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000009-",
        "AIMDBStatus.isActief": True,
        'AIMNaamObject.naam': 'C02',
        "AIMObject.assetId": {
            "DtcIdentificator.toegekendDoor": "AWV",
            "DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000009-"
        },
        "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPConsole",
        "NaampadObject.naampad": "A0000/A0000.WV/C02",
    }

    relatie_10 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000002-Bevestigin-000000000004-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000002-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-"
        }
    }
    relatie_11 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000006-Bevestigin-000000000002-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000006-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000002-"
        }
    }
    relatie_12 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000005-Bevestigin-000000000003-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000005-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000003-"
        }
    }
    relatie_13 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#Bevestiging",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000003-Bevestigin-000000000007-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VerlichtingstoestelLED",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000003-"
        },
        "RelatieObject.doel": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Armatuurcontroller",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000007-"
        }
    }
    relatie_14 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#HoortBij",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000004--HoortBij--000000000008-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000004-"
        },
        "RelatieObject.doel": {
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000008-"
        }
    }
    relatie_15 = {
        "@type": "https://grp.data.wegenenverkeer.be/ns/onderdeel#HoortBij",
        "@id": "https://data.awvvlaanderen.be/id/assetrelatie/000000000005--HoortBij--000000000009-",
        "RelatieObject.bron": {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVConsole",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000005-"
        },
        "RelatieObject.doel": {
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPConsole",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000009-"
        }
    }

    logging.debug(f'API Call made to get objects from oslo search endpoint using iterator, resource: {resource}')

    if resource == 'assets':
        yield from iter([a for a in [asset_1, asset_2, asset_3, asset_4, asset_5, asset_6, asset_7, asset_8, asset_9,
                                     asset_inactief]
                         if a['@id'][39:75] in filter_dict['uuid']])
    elif resource == 'assetrelaties':
        assetrelaties = [relatie_10, relatie_11, relatie_12, relatie_13, relatie_14, relatie_15]
        if 'uuid' in filter_dict:
            yield from iter([r for r in assetrelaties
                             if r['@id'][46:82] in filter_dict['uuid']])
        elif 'asset' in filter_dict:
            yield from iter([r for r in assetrelaties
                             if r['RelatieObject.bron']['@id'][39:75] in filter_dict['asset'] or
                             r['RelatieObject.doel']['@id'][39:75] in filter_dict['asset']])


fake_em_infra_importer = Mock(spec=EMInfraRestClient)
fake_em_infra_importer.get_objects_from_oslo_search_endpoint_using_iterator = fake_get_objects_from_oslo_search_endpoint_using_iterator


def test_asset_info_collector():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000001'])
    asset_node = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000001')
    assert asset_node.uuid == '00000000-0000-0000-0000-000000000001'


def test_asset_info_collector_inactive():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.collect_asset_info(uuids=['00000000-0000-0000-0000-000000000010'])
    asset_node = collector.collection.get_node_object_by_uuid('00000000-0000-0000-0000-000000000010')
    assert asset_node.uuid == '00000000-0000-0000-0000-000000000010'
    assert asset_node.active is False


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_a():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003'],
        pattern=[('uuids', 'of', 'a'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole', 'onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000003-Bevestigin-000000000007'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'}
    }


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_c():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000008', '00000000-0000-0000-0000-000000000009'],
        pattern=[('uuids', 'of', 'c'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
                 ('a', '-[r1]-', 'd'),
                 ('d', 'type_of', ['onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000003-Bevestigin-000000000007'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'}
    }


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_d():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000007'],
        pattern=[('uuids', 'of', 'd'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
                 ('a', '-[r1]-', 'd'),
                 ('d', 'type_of', ['onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000003-Bevestigin-000000000007'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'}
    }


def test_start_collecting_from_starting_uuids_using_pattern_giving_uuids_of_b():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.start_collecting_from_starting_uuids_using_pattern(
        starting_uuids=['00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000005'],
        pattern=[('uuids', 'of', 'b'),
                 ('a', 'type_of', ['onderdeel#VerlichtingstoestelLED']),
                 ('a', '-[r1]-', 'b'),
                 ('b', 'type_of', ['onderdeel#WVLichtmast', 'onderdeel#WVConsole']),
                 ('a', '-[r1]-', 'd'),
                 ('d', 'type_of', ['onderdeel#Armatuurcontroller']),
                 ('b', '-[r2]->', 'c'),
                 ('c', 'type_of', ['lgc:installatie#VPLMast', 'lgc:installatie#VPConsole']),
                 ('r1', 'type_of', ['onderdeel#Bevestiging']),
                 ('r2', 'type_of', ['onderdeel#HoortBij'])])

    assert collector.collection.short_uri_dict == {
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000003-Bevestigin-000000000007'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'}
    }


def test_start_collecting_from_starting_uuids():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.start_collecting_from_starting_uuids(
        starting_uuids=['00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003'])

    assert collector.collection.short_uri_dict == {
        'onderdeel#VerlichtingstoestelLED': {'00000000-0000-0000-0000-000000000002',
                                             '00000000-0000-0000-0000-000000000003'},
        'onderdeel#WVLichtmast': {'00000000-0000-0000-0000-000000000004'},
        'onderdeel#WVConsole': {'00000000-0000-0000-0000-000000000005'},
        'onderdeel#Armatuurcontroller': {'00000000-0000-0000-0000-000000000006',
                                         '00000000-0000-0000-0000-000000000007'},
        'lgc:installatie#VPLMast': {'00000000-0000-0000-0000-000000000008'},
        'lgc:installatie#VPConsole': {'00000000-0000-0000-0000-000000000009'},
        'onderdeel#Bevestiging': {'000000000002-Bevestigin-000000000004',
                                  '000000000006-Bevestigin-000000000002',
                                  '000000000005-Bevestigin-000000000003',
                                  '000000000003-Bevestigin-000000000007'},
        'onderdeel#HoortBij': {'000000000004--HoortBij--000000000008',
                               '000000000005--HoortBij--000000000009'}
    }


def test_reverse_relation_pattern():
    reversed1 = AssetInfoCollector.reverse_relation_pattern(('a', '-[r1]-', 'b'))
    assert reversed1 == ('b', '-[r1]-', 'a')

    reversed2 = AssetInfoCollector.reverse_relation_pattern(('a', '-[r1]->', 'b'))
    assert reversed2 == ('b', '<-[r1]-', 'a')

    reversed3 = AssetInfoCollector.reverse_relation_pattern(('a', '<-[r1]->', 'b'))
    assert reversed3 == ('b', '<-[r1]->', 'a')

    reversed4 = AssetInfoCollector.reverse_relation_pattern(('a', '<-[r1]-', 'b'))
    assert reversed4 == ('b', '-[r1]->', 'a')


def test_start_creating_report():
    fake_requester = Mock(spec=AbstractRequester)
    fake_requester.first_part_url = ''
    AssetInfoCollector.create_requester_with_settings = Mock(return_value=fake_requester)
    collector = AssetInfoCollector(auth_type=Mock(), env=Mock(), settings_path=Mock())
    collector.em_infra_importer = fake_em_infra_importer

    collector.collect_asset_info(
        uuids=['00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003',
               '00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000005',
               '00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000007',
               '00000000-0000-0000-0000-000000000008', '00000000-0000-0000-0000-000000000009'])
    collector.collect_relation_info(
        uuids=['000000000002-Bevestigin-000000000004', '000000000006-Bevestigin-000000000002',
               '000000000005-Bevestigin-000000000003', '000000000003-Bevestigin-000000000007',
               '000000000004--HoortBij--000000000008', '000000000005--HoortBij--000000000009'])

    expected_report = {
        'columns': [
            'aanlevering_id', 'aanlevering_naam', 'LED_toestel_uuid', 'LED_toestel_naam',
            'LED_toestel_naam_conform_conventie',
            'relatie_naar_armatuur_controller_aanwezig', 'armatuur_controller_uuid', 'armatuur_controller_naam',
            'armatuur_controller_naam_conform_conventie',
            'relatie_naar_drager_aanwezig', 'drager_uuid', 'drager_type', 'drager_naam', 'drager_naam_conform_conventie',
            'relatie_naar_legacy_drager_aanwezig', 'legacy_drager_uuid', 'legacy_drager_type', 'legacy_drager_naampad',
            'legacy_drager_naampad_conform_conventie', 'legacy_drager_LED_toestel_binnen_5_meter'],
        'index': [0, 0],
        'data': [
            ['01', 'DA-01', '00000000-0000-0000-0000-000000000002', 'A0000.A01.WV1',
             True,
             True, '00000000-0000-0000-0000-000000000006', 'A0000.A01.WV1.AC1',
             True,
             True, '00000000-0000-0000-0000-000000000004', 'WVLichtmast', 'A0000.A01', True,
             True, '00000000-0000-0000-0000-000000000008', 'VPLMast', 'A0000/A0000.WV/A01', True, math.nan],
            ['01', 'DA-01', '00000000-0000-0000-0000-000000000003', 'A0000.C02.WV1',
             True,
             True, '00000000-0000-0000-0000-000000000007', 'A0000.C02.WV1.AC1',
             True,
             True, '00000000-0000-0000-0000-000000000005', 'WVConsole', 'A0000.FOUT1', False,
             True, '00000000-0000-0000-0000-000000000009', 'VPConsole', 'A0000/A0000.WV/C02', True, math.nan]]}

    report = collector.start_creating_report('01', 'DA-01')

    assert report.to_dict('split') == expected_report


def test_is_conform_name_convention_toestel():
    assert AssetInfoCollector.is_conform_name_convention_toestel('A0000.A01.WV1')
    assert not AssetInfoCollector.is_conform_name_convention_toestel('A0000..WV1')
    assert not AssetInfoCollector.is_conform_name_convention_toestel('A0000.A01.AC1')
    assert not AssetInfoCollector.is_conform_name_convention_toestel('0000.A01.WV1')
    assert not AssetInfoCollector.is_conform_name_convention_toestel('A0000.A01')
    assert not AssetInfoCollector.is_conform_name_convention_toestel('A0000.A01.WV1.AC1')


def test_is_conform_name_convention_armatuur_controller():
    assert AssetInfoCollector.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV1.AC1', toestel_name='A0000.A01.WV1')
    assert AssetInfoCollector.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV1.AC2', toestel_name='A0000.A01.WV1')
    assert not AssetInfoCollector.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.AC1', toestel_name='A0000.A01.WV1')
    assert not AssetInfoCollector.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV1', toestel_name='A0000.A01.WV1.AC1')
    assert not AssetInfoCollector.is_conform_name_convention_armatuur_controller(
        controller_name='A0000.A01.WV2.AC1', toestel_name='A0000.A01.WV1')


def test_is_conform_name_convention_drager():
    assert AssetInfoCollector.is_conform_name_convention_drager(
        drager_name='A0000.A01', toestel_name='A0000.A01.WV1')
    assert AssetInfoCollector.is_conform_name_convention_drager(
        drager_name='A0000.A01', toestel_name='A0000.A01.WV2')
    assert not AssetInfoCollector.is_conform_name_convention_drager(
        drager_name='A0000.A01.WV1', toestel_name='A0000.A01')
    assert not AssetInfoCollector.is_conform_name_convention_drager(
        drager_name='A0000.A01.WV1.AC1', toestel_name='A0000.A01.WV1')
    assert not AssetInfoCollector.is_conform_name_convention_drager(
        drager_name='A0000.A02', toestel_name='A0000.A01.WV1')


def test_is_conform_name_convention_legacy_drager():
    assert AssetInfoCollector.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0000/A0000.WV/A01', installatie_nummer='A0000', lichtpunt_nummer='A01')
    assert AssetInfoCollector.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0001/A0001.WV/A02', installatie_nummer='A0001', lichtpunt_nummer='A02')
    assert not AssetInfoCollector.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0002/A0002.WV/A02', installatie_nummer='A0001', lichtpunt_nummer='A02')
    assert not AssetInfoCollector.is_conform_name_convention_legacy_drager(
        legacy_drager_naampad='A0001/A0001.WV/A02', installatie_nummer='A0001', lichtpunt_nummer='A01')

def test_get_installatie_nummer_from_name():
    assert AssetInfoCollector.get_installatie_nummer_from_name('A0000.A01.WV1') == 'A0000'
    assert AssetInfoCollector.get_installatie_nummer_from_name('0000.A01.WV1') == '0000'
    assert AssetInfoCollector.get_installatie_nummer_from_name('A0000.A01.WV1.AC1') == 'A0000'
    assert AssetInfoCollector.get_installatie_nummer_from_name('A0000') == ''
    assert AssetInfoCollector.get_installatie_nummer_from_name('') == ''
    assert AssetInfoCollector.get_installatie_nummer_from_name(None) == ''


def test_get_lichtpunt_nummer_from_name():
    assert AssetInfoCollector.get_lichtpunt_nummer_from_name('A0000.A01.WV1') == 'A01'
    assert AssetInfoCollector.get_lichtpunt_nummer_from_name('0000.01.WV1') == '01'
    assert AssetInfoCollector.get_lichtpunt_nummer_from_name('A0000.A01.WV1.AC1') == 'A01'
    assert AssetInfoCollector.get_lichtpunt_nummer_from_name('A0000') == ''
    assert AssetInfoCollector.get_lichtpunt_nummer_from_name('') == ''
    assert AssetInfoCollector.get_lichtpunt_nummer_from_name(None) == ''
