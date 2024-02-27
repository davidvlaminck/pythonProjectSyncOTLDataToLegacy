from API.AbstractRequester import AbstractRequester
from Domain.DavieDomain import ZoekTerm, Aanlevering


class DavieRestClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'davie-aanlevering/'

    def find_delivery_by_search_parameters(self, search_parameters: ZoekTerm) -> Aanlevering:
        response = self.requester.post(
            url='api/aanleveringen/zoek', json=search_parameters.to_dict()
        )
        if response.status_code != 200:
            print(response)
            raise RuntimeError(response.content.decode())

        response_string = response.content.decode()
        return Aanlevering.parse_raw(response_string)
