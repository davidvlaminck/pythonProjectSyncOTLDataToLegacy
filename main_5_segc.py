from pathlib import Path

from DataLegacySyncer import DataLegacySyncer
from Domain.AssetInfoCollector import AssetInfoCollector
from Domain.Enums import AuthType, Environment

import logging
import sys

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
state_db_path = Path('/home/davidlinux/Documents/AWV/resources/SyncOTLDataToLegacy_state.db')

if __name__ == '__main__':
    #collector = AssetInfoCollector(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)
    
    syncer = DataLegacySyncer(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD,
                              state_db_path=state_db_path)

    for a in syncer.emson_importer.get_assets_by_uuid_using_iterator(uuids=['00000453-56ce-4f8b-af44-960df526cb30']):
        print(a)

