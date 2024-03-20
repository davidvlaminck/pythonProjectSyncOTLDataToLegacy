from API.AbstractRequester import AbstractRequester
from Domain.DavieDomain import ZoekTerm, Aanlevering


class DavieRestClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url = f'{self.requester.first_part_url}davie-aanlevering/api/'

    def find_delivery_by_search_parameters(self, search_parameters: ZoekTerm) -> Aanlevering:
        response = self.requester.get(
            url='aanleveringen/8a73625d-8b5d-4c57-ba14-95dd4d15fb17'
        )
        if response.status_code != 200:
            print(response)
            raise RuntimeError(response.content.decode())

        response_string = response.content.decode()
        return Aanlevering.parse_raw(response_string)



        response = self.requester.post(
            url='aanleveringen/zoek', json=search_parameters.to_dict()
        )
        if response.status_code != 200:
            print(response)
            raise RuntimeError(response.content.decode())

        response_string = response.content.decode()
        return Aanlevering.parse_raw(response_string)
