import json

from API.AbstractRequester import AbstractRequester
from Domain.DavieDomain import ZoekTerm, Aanlevering, AanleveringResultaat


class DavieRestClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url = f'{self.requester.first_part_url}'

    def find_delivery_by_reference(self, reference: str) -> Aanlevering:
        response = self.requester.get(
            url=f'aanleveringen/{reference}'
        )
        if response.status_code != 200:
            print(response)
            raise RuntimeError(response.content.decode())

        response_string = response.content.decode()
        ar = AanleveringResultaat.model_validate_json(response_string)
        return ar.aanlevering

    def get_delivery_by_id(self, id: str) -> Aanlevering:
        response = self.requester.get(
            url=f'aanleveringen/{id}'
        )
        if response.status_code != 200:
            print(response)
            raise RuntimeError(response.content.decode())

        response_string = response.content.decode()
        ar = AanleveringResultaat.model_validate_json(response_string)
        return ar.aanlevering
