from pathlib import Path

from DataLegacySyncer import DataLegacySyncer
from Domain.AssetInfoCollector import AssetInfoCollector
from Domain.Enums import AuthType, Environment, Toestand

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

    syncer = DataLegacySyncer(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.TEI,
                              state_db_path=state_db_path)
    # installatie = syncer.em_infra_client.get_installatie_by_id('00000453-56ce-4f8b-af44-960df526cb30')
    # print(installatie)
    # installatie_update = syncer.em_infra_client.create_installatie_update_from_installatie(installatie)
    # installatie_update.toestand = Toestand.UIT_GEBRUIK
    # syncer.em_infra_client.put_installatie_by_id(id='00000453-56ce-4f8b-af44-960df526cb30',
    #                                              changed_installatie=installatie_update)
    # installatie = syncer.em_infra_client.get_installatie_by_id('00000453-56ce-4f8b-af44-960df526cb30')
    # print(installatie)

    # locatie = syncer.em_infra_client.get_locatie_by_installatie_id('00000453-56ce-4f8b-af44-960df526cb30')
    # print(locatie)
    # locatie_update = syncer.em_infra_client.create_locatie_kenmerk_update_from_locatie_kenmerk(locatie)
    # locatie_update.locatie.coordinaten.x += 1
    # locatie_update.locatie.coordinaten.y += 1
    #
    # syncer.em_infra_client.put_locatie_kenmerk_update_by_id(id='00000453-56ce-4f8b-af44-960df526cb30',
    #                                                         locatie_kenmerk_update=locatie_update)
    #
    # locatie = syncer.em_infra_client.get_locatie_by_installatie_id('00000453-56ce-4f8b-af44-960df526cb30')
    # print(locatie)

    eig = syncer.em_infra_client.get_eigenschapwaarden_by_id('0000da03-06f3-4a22-a609-d82358c62273')
    print(eig)

    update_eig_dto = syncer.em_infra_client.create_update_eigenschappen_from_update_dict(
        update_dict={
        'aantal_armen': '4',
        'aantal_te_verlichten_rijvakken_LED': 'R1',
        'aantal_verlichtingstoestellen': 4,
        'armlengte': '1,5',
        'contractnummer_levering_LED': 'MDN58',
        'datum_installatie_LED': '2020-01-01',
        'kleurtemperatuur_LED': 'K3000',
        'LED_verlichting': True,
        'drager_buiten_gebruik': False,
        'lichtpunthoogte_tov_rijweg': 6,
        'lumen_pakket_LED': 10000,
        'merk_en_type_armatuurcontroller_1': 'merk 1 model 1',
        'merk_en_type_armatuurcontroller_2': 'merk 2 model 2',
        'merk_en_type_armatuurcontroller_3': 'merk 3 model 3',
        'merk_en_type_armatuurcontroller_4': 'merk 4 model 4',
        'overhang_LED': 'O+1',
        'paalhoogte': '12,00',
        'RAL_kleur': '7038',
        'serienummer_armatuurcontroller_1': '1234561',
        'serienummer_armatuurcontroller_2': '1234562',
        'serienummer_armatuurcontroller_3': '1234563',
        'serienummer_armatuurcontroller_4': '1234564',
        'verlichtingsniveau_LED': 'M3',
        'verlichtingstoestel_merk_en_type': 'Schreder Ampera',
        'verlichtingstoestel_systeemvermogen': 100,
        'verlichtingstype': 'hoofdbaan'
    }, short_uri='lgc:installatie#VPLMast')
    syncer.em_infra_client.patch_eigenschapwaarden('0000da03-06f3-4a22-a609-d82358c62273', update_eig_dto)
    eig = syncer.em_infra_client.get_eigenschapwaarden_by_id('0000da03-06f3-4a22-a609-d82358c62273')
    print(eig)