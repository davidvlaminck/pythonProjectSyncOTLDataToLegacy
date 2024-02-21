import json
from typing import Iterator, Generator

from requests import Response

from AbstractRequester import AbstractRequester
from EMInfraDomain import FeedPage, FeedProxyPage
from ZoekParameterOTL import ZoekParameterOTL


class EMInfraImporter:
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

    def print_feed_page(self) -> FeedProxyPage:
        response = self.requester.get(
            url='feedproxy/feed/assets')
        if response.status_code != 200:
            print(response)
            raise ProcessLookupError(response.content.decode("utf-8"))

        response_string = response.content.decode("utf-8")
        feed_page = FeedProxyPage.parse_raw(response_string)
        print(feed_page)
        return feed_page
