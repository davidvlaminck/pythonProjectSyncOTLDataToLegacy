import logging
import time
import traceback
from typing import Tuple, List

from API.DavieRestClient import DavieRestClient
from API.EMInfraRestClient import EMInfraRestClient
from Database.DbManager import DbManager
from Domain.EMInfraDomain import FeedProxyPage, ProxyEntryObject


class DeliveryFinder:
    def __init__(self, em_infra_client: EMInfraRestClient, davie_client: DavieRestClient, db_manager: DbManager):
        self.em_infra_client = em_infra_client
        self.davie_client = davie_client
        self.db_manager = db_manager

    def get_current_feedproxy_page_and_event(self) -> tuple[str, str]:
        feedproxy_page_number = self.db_manager.get_state_variable('feedproxy_page')
        if feedproxy_page_number is None:
            return self.get_page_number_and_event_from_api()
            # fetch the current feedproxy page
        return (str(self.db_manager.get_state_variable('feedproxy_page')),
                str(self.db_manager.get_state_variable('feedproxy_event_id')))

    def get_page_number_and_event_from_api(self) -> tuple[str, str]:
        feedproxy_page = self.em_infra_client.get_current_feed_page()
        return self.get_page_number_and_event_from_page(feedproxy_page)

    def get_page_number_and_event_from_page(self, feedproxy_page: FeedProxyPage) -> tuple[str, str]:
        last_event_id = feedproxy_page.entries[0].id
        feedproxy_page_number_href = next(l for l in feedproxy_page.links if l.rel == 'self').href  # '/279275/100'
        feedproxy_page_number = feedproxy_page_number_href.split('/')[1]
        self.db_manager.set_state_variable('feedproxy_page', feedproxy_page_number)
        self.db_manager.set_state_variable('feedproxy_event_id', last_event_id)
        return feedproxy_page_number, last_event_id

    def find_deliveries_to_sync(self):
        while True:
            try:
                current_feedproxy_page, current_feedproxy_event = self.get_current_feedproxy_page_and_event()
                #
                # current_feedproxy_page = '279270'
                # current_feedproxy_event = 'c8632e7b-3b6c-4a44-80ee-b7d94681bec3'

                print(current_feedproxy_page, current_feedproxy_event)
                proxy_feed_page = self.fetch_current_feed_page(current_feedproxy_page)
                if self.is_last_event_in_feedproxy(proxy_feed_page=proxy_feed_page, event_id=current_feedproxy_event):
                    logging.info('No new events found, sleeping for 30 seconds')
                    self.sleep(30)
                    continue

                events, feed_page_number, event_id = self.find_events_with_context(
                    current_feedproxy_event=current_feedproxy_event, current_feedproxy_page=current_feedproxy_page,
                    proxy_feed_page=proxy_feed_page)
                events_to_process = self.get_events_ready_to_process(events=events)
                self.save_events_to_process_to_db(events_to_process, feed_page_number, event_id)

            except Exception as e:
                print(e)
                print(traceback.print_exc())
                logging.error(e)
                self.sleep(10)

    # 1) collecting assets to check
    #     - [ ] use state db to check position in feedproxy
    #     - [ ] read feedproxy and capture events
    #     - [ ] only capture events that are applicable to the correct assettypes (use base64 encoded short_uri)
    #     - [ ] only capture events that have a context-id
    # 2) check the context-id
    #     - [ ] check to see if aanlevering is already being tracked
    #     - [ ] use /eventcontexts/{uuid} to get the description
    #     - [ ] use the description to search in the DAVIE API using field "vrijeZoekterm"
    #     - [ ] filter using "aanleveringnummer" and "dossierNummer" ("VWT-CEW-2020-009-5", ...)
    #     - [ ] save the aanlevering uuid

    def get_additional_attributes_of_deliveries(self) -> Tuple[str, str]:
        # find aanlevering in db without davie_uuid or referentie
        pass

    @classmethod
    def sleep(cls, seconds: int):
        time.sleep(seconds)

    def fetch_current_feed_page(self, current_feedproxy_page: str) -> FeedProxyPage:
        return self.em_infra_client.get_feed_page_by_number(page_number=current_feedproxy_page)

    @classmethod
    def is_last_event_in_feedproxy(cls, proxy_feed_page: FeedProxyPage, event_id: str) -> bool:
        return (proxy_feed_page.entries[0].id == event_id and
                next((l for l in proxy_feed_page.links if l.rel == 'previous'), None) is None)

    def find_events_with_context(self, current_feedproxy_event: str, current_feedproxy_page: str, 
                                 proxy_feed_page: FeedProxyPage, batch_page_size: int = 5
                                 ) -> tuple[list[ProxyEntryObject], str, str]:
        collected_events = []
        filtered_asset_types = {
            'b25kZXJkZWVsI1dWTGljaHRtYXN0',  # 'WVLichtmast'
            'b25kZXJkZWVsI1ZlcmxpY2h0aW5nc3RvZXN0ZWxMRUQ',  # onderdeel#VerlichtingstoestelLED
            'b25kZXJkZWVsI1dWQ29uc29sZQ',  # onderdeel#WVConsole
            'b25kZXJkZWVsI0FybWF0dXVyY29udHJvbGxlcg',  # onderdeel#Armatuurcontroller
            'bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q',  # lgc:installatie#VPLMast
            'bGdjOmluc3RhbGxhdGllI1ZQQ29uc29sZQ'  # lgc:installatie#VPConsole
        }
        page_count = 0
        while True:
            if proxy_feed_page is None:
                proxy_feed_page = self.fetch_current_feed_page(current_feedproxy_page)

            entries = reversed(proxy_feed_page.entries)
            for entry in entries:
                if current_feedproxy_event is not None:
                    if entry.id == current_feedproxy_event:
                        current_feedproxy_event = None
                    continue

                if entry.content.value.context_id is None:
                    continue
                asset_types = [t[37:] for t in entry.content.value.aim_ids]
                if any(t in filtered_asset_types for t in asset_types):
                    collected_events.append(entry)

            current_feedproxy_event = proxy_feed_page.entries[0].id
            if page_count >= batch_page_size:
                break
            page_count += 1

            previous_link = next((l for l in proxy_feed_page.links if l.rel == 'previous'), None)
            if previous_link is None:
                break

            proxy_feed_page = None
            current_feedproxy_page = previous_link.href.split('/')[1]
            current_feedproxy_event = None

        return collected_events, current_feedproxy_page, current_feedproxy_event

    def get_events_ready_to_process(self, events):
        pass

    def save_events_to_process_to_db(self, events_to_process, feed_page_number, event_id):
        pass
