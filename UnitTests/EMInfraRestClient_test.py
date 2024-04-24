from API.EMInfraRestClient import EMInfraRestClient
from Domain.EMInfraDomain import ListUpdateDTOKenmerkEigenschapValueUpdateDTO, KenmerkEigenschapValueUpdateDTO


def test_create_update_eigenschappen_from_update_dict_empty_dict():
    result_dto = EMInfraRestClient.create_update_eigenschappen_from_update_dict({}, 'test')
    assert isinstance(result_dto, ListUpdateDTOKenmerkEigenschapValueUpdateDTO)
    assert result_dto.data == []


def test_create_update_eigenschappen_from_update_dict_dict_with_one():
    update_dict = {'aantal_te_verlichten_rijvakken_LED': 'R1'}
    result_dto = EMInfraRestClient.create_update_eigenschappen_from_update_dict(update_dict=update_dict,
                                                                                short_uri='lgc:installatie#VPLMast')
    assert isinstance(result_dto, ListUpdateDTOKenmerkEigenschapValueUpdateDTO)
    assert isinstance(result_dto.data[0], KenmerkEigenschapValueUpdateDTO)
    assert result_dto.data[0].eigenschap.uuid == 'da9bb59b-d949-42fc-81e4-8f978ec917a6'
    assert result_dto.data[0].kenmerkType.uuid == 'bfd35cc8-a787-4517-bcb3-895126a8414c'
    assert result_dto.data[0].typedValue.value == '1'
    assert result_dto.data[0].typedValue.type == 'text'



