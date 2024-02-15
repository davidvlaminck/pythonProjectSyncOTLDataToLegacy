from typing import Generator

from InfoObject import InfoObject, NodeInfoObject, RelationInfoObject


class AssetCollection:
    def __init__(self):
        self.object_dict: dict[str: InfoObject] = {}

    def add_node(self, d) -> None:
        uuid = d['uuid']
        self.check_if_exists(uuid)

        info_object = NodeInfoObject(uuid=uuid, short_type=d['typeURI'], attr_dict=d)
        self.object_dict[d['uuid']] = info_object

    def add_relation(self, d) -> None:
        uuid = d['uuid']
        self.check_if_exists(uuid)

        bron_object = self.get_object_by_uuid(d['bron'])
        if bron_object is None:
            raise ValueError(f"Bron object with uuid {d['bron']} does not exist in collection.")
        doel_object = self.get_object_by_uuid(d['doel'])
        if doel_object is None:
            raise ValueError(f"Doel object with uuid {d['doel']} does not exist in collection.")

        info_object = RelationInfoObject(uuid=uuid, short_type=d['typeURI'], attr_dict=d)
        self.object_dict[d['uuid']] = info_object

    def get_object_by_uuid(self, uuid: str) -> InfoObject | None:
        return self.object_dict.get(uuid)

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
