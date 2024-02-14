import pytest

from AssetCollection import AssetCollection


def test_add_node_and_get_object_by_uuid():
    collection = AssetCollection()
    d = {
        'uuid': '0001',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'
    }
    collection.add_node(d)

    node = collection.get_object_by_uuid('0001')
    assert node == d


def test_add_relation_and_get_object_by_uuid():
    collection = AssetCollection()
    m1 = {'uuid': '0001',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    m2 = {'uuid': '0002',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    r = {
        'uuid': '0001-0002',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging',
        'bron': '0001',
        'doel': '0002'
    }
    collection.add_node(m1)
    collection.add_node(m2)
    collection.add_relation(r)

    node = collection.get_object_by_uuid('0001-0002')
    assert node == r


def test_get_nodes():
    collection = AssetCollection()
    m1 = {'uuid': '0001',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    m2 = {'uuid': '0002',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    r = {
        'uuid': '0001-0002',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging',
        'bron': '0001',
        'doel': '0002'
    }
    collection.add_node(m1)
    collection.add_node(m2)
    collection.add_relation(r)

    node_list = list(collection.get_nodes())
    assert node_list == [m1, m2]


def test_get_relations():
    collection = AssetCollection()
    m1 = {'uuid': '0001',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    m2 = {'uuid': '0002',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    r = {
        'uuid': '0001-0002',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging',
        'bron': '0001',
        'doel': '0002'
    }
    collection.add_node(m1)
    collection.add_node(m2)
    collection.add_relation(r)

    node_list = list(collection.get_relations())
    assert node_list == [r]


def test_add_invalid_relation_missing_node():
    collection = AssetCollection()
    m1 = {'uuid': '0001',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    r = {
        'uuid': '0001-0002',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging',
        'bron': '0001',
        'doel': '0002'
    }
    collection.add_node(m1)
    with pytest.raises(ValueError):
        collection.add_relation(r)
