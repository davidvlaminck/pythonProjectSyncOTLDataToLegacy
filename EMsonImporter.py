import json
from typing import Iterator, Generator

from requests import Response

from AbstractRequester import AbstractRequester
from ZoekParameterOTL import ZoekParameterOTL


class EMsonImporter:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'emson/'

    def get_assets_by_uuid_using_iterator(
            self,
            cursor: str | None = None,
            size: int = 100,
            uuids: [str] = None) -> Generator[Iterator[dict], None, None]:

        url = 'api/otl/assets/search'

        while True:
            otl_zoekparameter = ZoekParameterOTL(size=size, from_cursor=cursor, filter_dict={'uuid': list(uuids)})
            json_data = otl_zoekparameter.to_dict()

            response = self.requester.post(url=url, data=json_data)

            decoded_string = response.content.decode()
            graph = json.loads(decoded_string)
            headers = dict(response.headers)
            
            yield graph['@graph']
            if 'em-paging-next-cursor' not in headers:
                break
            cursor = headers['em-paging-next-cursor']
