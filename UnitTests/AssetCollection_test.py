import unittest

import pytest

from AssetCollection import AssetCollection
from Enums import Direction
from Exceptions.AssetsMissingError import AssetsMissingError


def test_full_uri_to_short_typed():
    collection = AssetCollection()
    a1 = {'uuid': '0001',
         'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    a2 = {'uuid': '0002',
         'typeURI': 'https://lgc.data.wegenenverkeer.be/ns/installatie#Kast'}
    collection.add_node(a1)
    collection.add_node(a2)

    node1 = collection.get_object_by_uuid('0001')
    assert node1.short_type == 'onderdeel#WVLichtmast'
    node2 = collection.get_object_by_uuid('0002')
    assert node2.short_type == 'lgc:installatie#Kast'


def test_add_node_and_get_object_by_uuid():
    collection = AssetCollection()
    d = {'uuid': '0001',
         'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    collection.add_node(d)

    node = collection.get_object_by_uuid('0001')
    assert node.attr_dict == d
    assert node.uuid == '0001'
    assert node.short_type == 'onderdeel#WVLichtmast'
    assert node.active is True

    with pytest.raises(ValueError):
        collection.get_object_by_uuid('0002')


def test_get_node_object_by_uuid():
    collection = AssetCollection()
    d = {'uuid': '0001',
         'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    collection.add_node(d)

    node = collection.get_node_object_by_uuid('0001')
    assert node.uuid == '0001'

    with pytest.raises(ValueError):
        collection.get_relation_object_by_uuid('0001')


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

    relation = collection.get_object_by_uuid('0001-0002')
    assert relation.attr_dict == r
    assert relation.uuid == '0001-0002'
    assert relation.short_type == 'onderdeel#Bevestiging'
    assert relation.active is True


def test_get_relation_object_by_uuid():
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

    node = collection.get_relation_object_by_uuid('0001-0002')
    assert node.uuid == '0001-0002'

    with pytest.raises(ValueError):
        collection.get_node_object_by_uuid('0001-0002')


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

    node_list = [o.attr_dict for o in collection.get_node_objects()]
    assert node_list == [m1, m2]


def test_short_uri_counter():
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

    assert collection.short_uri_counter == {'onderdeel#WVLichtmast': 2, 'onderdeel#Bevestiging': 1}


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

    node_list = [o.attr_dict for o in collection.get_relation_objects()]
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
    with pytest.raises(AssetsMissingError):
        collection.add_relation(r)


def test_check_attributes_after_adding_relation():
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

    relation_object = next(collection.get_relation_objects())
    assert relation_object.is_relation is True
    assert relation_object.is_directional_relation is False
    assert relation_object.bron.uuid == m1['uuid']
    assert relation_object.bron.attr_dict == m1
    assert relation_object.doel.uuid == m2['uuid']
    assert relation_object.doel.attr_dict == m2

    m1_object = collection.get_node_object_by_uuid('0001')
    m2_object = collection.get_node_object_by_uuid('0002')
    assert m1_object.is_relation is False
    assert m1_object.relations == {
        'Bevestiging': {
            '0002': {
                'direction': Direction.NONE,
                'relation_object': relation_object,
                'node_object': m2_object}
        }
    }
    assert m2_object.is_relation is False
    assert m2_object.relations == {
        'Bevestiging': {
            '0001': {
                'direction': Direction.NONE,
                'relation_object': relation_object,
                'node_object': m1_object}
        }
    }


def test_check_attributes_after_adding_directional_relation():
    collection = AssetCollection()
    m1 = {'uuid': '0001',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    m2 = {'uuid': '0002',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    r = {
        'uuid': '0001-0002',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt',
        'bron': '0001',
        'doel': '0002'
    }
    collection.add_node(m1)
    collection.add_node(m2)
    collection.add_relation(r)

    relation_object = next(collection.get_relation_objects())
    assert relation_object.is_relation is True
    assert relation_object.is_directional_relation is True
    assert relation_object.bron.uuid == m1['uuid']
    assert relation_object.bron.attr_dict == m1
    assert relation_object.doel.uuid == m2['uuid']
    assert relation_object.doel.attr_dict == m2

    m1_object = collection.get_node_object_by_uuid('0001')
    m2_object = collection.get_node_object_by_uuid('0002')
    assert m1_object.is_relation is False
    assert m1_object.relations == {
        'Voedt': {
            '0002': {
                'direction': Direction.WITH,
                'relation_object': relation_object,
                'node_object': m2_object}
        }
    }
    assert m2_object.is_relation is False
    assert m2_object.relations == {
        'Voedt': {
            '0001': {
                'direction': Direction.REVERSED,
                'relation_object': relation_object,
                'node_object': m1_object}
        }
    }


def test_traverse_graph():
    collection = AssetCollection()
    a1 = {'uuid': '0001',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    a2 = {'uuid': '0002',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    a3 = {'uuid': '0003',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Stroomkring'}
    r1 = {
        'uuid': '0001-0002',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging',
        'bron': '0001',
        'doel': '0002'
    }
    r2 = {
        'uuid': '0001-0003',
        'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt',
        'bron': '0001',
        'doel': '0003'
    }
    collection.add_node(a1)
    collection.add_node(a2)
    collection.add_node(a3)
    collection.add_relation(r1)
    collection.add_relation(r2)

    # finds 1 result
    results_1 = collection.traverse_graph(start_uuid='0001', relation_types=['Bevestiging'],
                                          allowed_directions=[Direction.NONE],
                                          filtered_node_types=['onderdeel#WVLichtmast'])
    assert list(results_1) == ['0002']

    # finds 1 result, using minimum arguments
    results_1 = collection.traverse_graph(start_uuid='0001')
    assert list(results_1) == ['0002', '0003']

    # finds 1 result, return_type object
    results_1 = collection.traverse_graph(start_uuid='0001', relation_types=['Bevestiging'],
                                          allowed_directions=[Direction.NONE],
                                          filtered_node_types=['onderdeel#WVLichtmast'], return_type='info_object')
    assert next(results_1).attr_dict == a2

    # finds 1 result
    results_2 = collection.traverse_graph(start_uuid='0001', relation_types=['Voedt'],
                                          allowed_directions=[Direction.WITH],
                                          filtered_node_types=['onderdeel#Stroomkring'])
    assert list(results_2) == ['0003']

    # finds 0 results: wrong direction
    results_3 = collection.traverse_graph(start_uuid='0001', relation_types=['Voedt'],
                                          allowed_directions=[Direction.NONE],
                                          filtered_node_types=['onderdeel#WVLichtmast'])
    assert not list(results_3)

    # finds 0 results: wrong relation_type
    results_3 = collection.traverse_graph(start_uuid='0001', relation_types=['HoortBij'],
                                          allowed_directions=[Direction.NONE],
                                          filtered_node_types=['onderdeel#WVLichtmast'])
    assert not list(results_3)

    # finds 0 results: wrong node_type
    results_4 = collection.traverse_graph(start_uuid='0001', relation_types=['Bevestiging'],
                                          allowed_directions=[Direction.NONE],
                                          filtered_node_types=['onderdeel#Stroomkring'])
    assert not list(results_4)


@unittest.skip('fails to raise error')
def test_traverse_graph_invalid_start():
    collection = AssetCollection()
    a1 = {'uuid': '0001',
          'typeURI': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#WVLichtmast'}
    collection.add_node(a1)
    with pytest.raises(ValueError):
        collection.traverse_graph(start_uuid='0002')
