from collections import Counter
from typing import Generator

from Enums import Direction
from InfoObject import InfoObject, NodeInfoObject, RelationInfoObject, full_uri_to_short_type, is_directional_relation


class AssetCollection:
    def __init__(self):
        self.object_dict: dict[str: InfoObject] = {}
        self.short_uri_counter = Counter()

    def add_node(self, d) -> None:
        uuid = d['uuid']
        self.check_if_exists(uuid)

        short_uri = full_uri_to_short_type(d['typeURI'])

        info_object = NodeInfoObject(uuid=uuid, short_type=short_uri, attr_dict=d)
        self.object_dict[d['uuid']] = info_object
        self.short_uri_counter.update([short_uri])


    def add_relation(self, d) -> None:
        uuid = d['uuid']
        self.check_if_exists(uuid)

        bron_object = self.get_node_object_by_uuid(d['bron'])
        if bron_object is None:
            raise ValueError(f"Bron object with uuid {d['bron']} does not exist in collection.")
        doel_object = self.get_node_object_by_uuid(d['doel'])
        if doel_object is None:
            raise ValueError(f"Doel object with uuid {d['doel']} does not exist in collection.")

        short_type_relation = full_uri_to_short_type(d['typeURI'])
        relation_name = short_type_relation.split('#')[-1]
        relation_info_object = RelationInfoObject(uuid=uuid, short_type=short_type_relation, attr_dict=d,
                                                  bron=bron_object, doel=doel_object)

        if is_directional_relation(short_type_relation):
            direction_1 = Direction.WITH
            direction_2 = Direction.REVERSED
        else:
            direction_1 = Direction.NONE
            direction_2 = Direction.NONE

        if relation_name not in bron_object.relations:
            bron_object.relations[relation_name] = {}
        bron_object.relations[relation_name][doel_object.uuid] = {
            'direction': direction_1,
            'relation_object': relation_info_object,
            'node_object': doel_object
        }

        if relation_name not in doel_object.relations:
            doel_object.relations[relation_name] = {}
        doel_object.relations[relation_name][bron_object.uuid] = {
            'direction': direction_2,
            'relation_object': relation_info_object,
            'node_object': bron_object
        }

        self.object_dict[d['uuid']] = relation_info_object
        self.short_uri_counter.update([short_type_relation])

    def get_object_by_uuid(self, uuid: str) -> InfoObject | None:
        return self.object_dict.get(uuid)

    def get_node_object_by_uuid(self, uuid: str) -> NodeInfoObject | None:
        o = self.object_dict.get(uuid)
        if o is None:
            return None
        if o.is_relation:
            raise ValueError(f"Object with uuid {uuid} is a relation, not a node.")
        return o

    def get_relation_object_by_uuid(self, uuid: str) -> RelationInfoObject | None:
        o = self.object_dict.get(uuid)
        if o is None:
            return None
        if not o.is_relation:
            raise ValueError(f"Object with uuid {uuid} is a node, not a relation.")
        return o

    def get_attribute_dict_by_uuid(self, uuid: str) -> dict | None:
        o = self.get_object_by_uuid(uuid)
        return None if o is None else o.attr_dict

    def get_node_objects(self) -> Generator[NodeInfoObject, None, None]:
        yield from [node for node in self.object_dict.values() if not node.is_relation]

    def get_relation_objects(self) -> Generator[RelationInfoObject, None, None]:
        yield from [node for node in self.object_dict.values() if node.is_relation]

    def check_if_exists(self, uuid: str):
        existing = self.object_dict.get(uuid)
        if existing is not None:
            raise ValueError(f"Node with uuid {uuid} already exists in collection.")

    def traverse_graph(self, start_uuid: str, relation_types: [str] = None, allowed_directions: [Direction] = None,
                       filtered_node_types: [str] = None, return_type: str = 'uuid'
                       ) -> Generator[str | InfoObject, None, None]:
        starting_object = self.get_node_object_by_uuid(start_uuid)
        if starting_object is None:
            raise ValueError(f"Node with uuid {start_uuid} does not exist in collection.")

        if relation_types is None or len(relation_types) == 0:
            relation_types = list(starting_object.relations.keys())

        if allowed_directions is None or len(allowed_directions) == 0:
            allowed_directions = [Direction.WITH, Direction.REVERSED, Direction.NONE]

        if filtered_node_types is None or len(filtered_node_types) == 0:
            filtered_node_types = list(self.short_uri_counter.keys())

        if return_type not in ['uuid', 'info_object']:
            raise ValueError(f"return_type {return_type} is not supported.")

        for relation_type in relation_types:
            if relation_type not in starting_object.relations:
                continue
            for target_uuid, relation_info in starting_object.relations[relation_type].items():
                if (relation_info['direction'] in allowed_directions and
                        relation_info['node_object'].short_type in filtered_node_types):
                    if return_type == 'uuid':
                        yield target_uuid
                    else:
                        yield relation_info['node_object']
            
        
        
