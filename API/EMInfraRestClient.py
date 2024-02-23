import json
from typing import Generator

from requests import Response

from API.AbstractRequester import AbstractRequester
from Domain.EMInfraDomain import FeedProxyPage
from Domain.ZoekParameterOTL import ZoekParameterOTL


class EMInfraRestClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'eminfra/'

    def get_objects_from_oslo_search_endpoint_using_iterator(
            self, resource: str,
            cursor: str | None = None,
            size: int = 100,
            filter_dict: dict = None) -> Generator[dict, None, None]:
        while True:
            response = self.get_objects_from_oslo_search_endpoint(
                resource=resource, cursor=cursor, size=size, filter_dict=filter_dict)

            decoded_string = response.content.decode()
            graph = json.loads(decoded_string)
            headers = dict(response.headers)

            yield from graph['@graph']
            if 'em-paging-next-cursor' not in headers:
                break
            cursor = headers['em-paging-next-cursor']

    def get_objects_from_oslo_search_endpoint(self, resource: str,
                                              cursor: str | None = None,
                                              size: int = 100,
                                              filter_dict: dict = None) -> Response:
        url = f'core/api/otl/{resource}/search'
        otl_zoekparameter = ZoekParameterOTL(size=size, from_cursor=cursor, filter_dict=filter_dict)

        if resource == 'agents':
            otl_zoekparameter.expansion_field_list = ['contactInfo']

        json_data = otl_zoekparameter.to_dict()

        return self.requester.post(url=url, json=json_data)

    def get_current_feed_page(self) -> FeedProxyPage:
        response = self.requester.get(
            url='feedproxy/feed/assets')
        if response.status_code != 200:
            print(response)
            raise RuntimeError(response.content.decode())

        response_string = response.content.decode()
        return FeedProxyPage.parse_raw(response_string)

    def get_feed_page_by_number(self, page_number: str) -> FeedProxyPage:
        response = self.requester.get(
            url=f'feedproxy/feed/assets/{page_number}/100')
        if response.status_code != 200:
            print(response)
            raise RuntimeError(response.content.decode())

        response_string = response.content.decode()
        return FeedProxyPage.parse_raw(response_string)
