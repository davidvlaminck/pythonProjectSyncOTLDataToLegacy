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
    # collector = AssetInfoCollector(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)

    syncer = DataLegacySyncer(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD,
                              state_db_path=state_db_path)
    # ref = syncer.davie_client.find_delivery_by_reference('DA-2024-06930 (iteratie 1)')
    # print(ref)
    ref = syncer.sync_specific_deliveries(['DA-2023-02080', 'DA-2023-02231', 'DA-2024-14615'])
    print(ref)

    # response = collector.emson_importer.requester.get(url='api/otl/assets')
    # decoded_string = response.content.decode()
    # print(decoded_string)

    # collector.print_feed_page()
    #
    # for asset in collector.get_assets_by_uuids(uuids=["e0346d27-3dd8-413c-8f9c-8dad4963da8a"]):
    #     print(asset)
