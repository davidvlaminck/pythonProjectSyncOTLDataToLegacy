import json
from typing import Generator

from requests import Response

from API.AbstractRequester import AbstractRequester
from Domain.EMInfraDomain import FeedProxyPage
from Domain.ZoekParameterOTL import ZoekParameterOTL


class DavieRestClient:
    def __init__(self, requester: AbstractRequester):
        self.requester = requester
        self.requester.first_part_url += 'davie-aanleveringen/'
