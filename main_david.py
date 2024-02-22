from pathlib import Path

from Domain.AssetInfoCollector import AssetInfoCollector
from Domain.Enums import AuthType, Environment


settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')

if __name__ == '__main__':
    collector = AssetInfoCollector(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)

    # response = collector.emson_importer.requester.get(url='api/otl/assets')
    # decoded_string = response.content.decode()
    # print(decoded_string)

    collector.print_feed_page()
    #
    # for asset in collector.get_assets_by_uuids(uuids=["e0346d27-3dd8-413c-8f9c-8dad4963da8a"]):
    #     print(asset)

