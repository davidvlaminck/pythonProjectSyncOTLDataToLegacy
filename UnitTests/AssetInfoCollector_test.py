from unittest.mock import Mock

import pytest

from AbstractRequester import AbstractRequester
from AssetInfoCollector import AssetInfoCollector
from EMInfraImporter import EMInfraImporter


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
    if resource == 'assets':
        yield from iter([a for a in [asset_1, asset_inactief]
                         if a['@id'][39:75] in filter_dict['uuid']])


fake_em_infra_importer = Mock(spec=EMInfraImporter)
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
