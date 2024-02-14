import json
from pathlib import Path
from typing import Generator

from AbstractRequester import AbstractRequester
from EMInfraImporter import EMInfraImporter
from EMsonImporter import EMsonImporter
from Enums import AuthType, Environment
from RequesterFactory import RequesterFactory


class AssetCollection:
    def __init__(self):
        self.object_dict = {}

    def add_node(self, d) -> None:
        uuid = d['uuid']
        self.check_if_exists(uuid)

        self.object_dict[d['uuid']] = dict(d)

    def add_relation(self, d) -> None:
        uuid = d['uuid']
        self.check_if_exists(uuid)

        bron_object = self.get_object_by_uuid(d['bron'])
        if bron_object is None:
            raise ValueError(f"Bron object with uuid {d['bron']} does not exist in collection.")
        doel_object = self.get_object_by_uuid(d['doel'])
        if doel_object is None:
            raise ValueError(f"Doel object with uuid {d['doel']} does not exist in collection.")
        self.object_dict[d['uuid']] = dict(d)

    def get_object_by_uuid(self, uuid: str) -> dict:
        return self.object_dict.get(uuid)

    def get_nodes(self) -> Generator[dict, None, None]:
        yield from [node for node in self.object_dict.values() if 'bron' not in node]

    def get_relations(self) -> Generator[dict, None, None]:
        yield from [node for node in self.object_dict.values() if 'bron' in node]

    def check_if_exists(self, uuid: str):
        existing = self.object_dict.get(uuid)
        if existing is not None:
            raise ValueError(f"Node with uuid {uuid} already exists in collection.")
