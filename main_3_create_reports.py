import logging
import sys
from pathlib import Path

from DataLegacySyncer import DataLegacySyncer
from Domain.Enums import AuthType, Environment

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
    syncer = DataLegacySyncer(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD,
                              state_db_path=state_db_path)
    syncer.collect_and_create_specific_reports(['DA-2024-14615'])