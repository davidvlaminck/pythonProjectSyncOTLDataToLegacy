import logging
import time
import traceback
from typing import Tuple

from API.DavieRestClient import DavieRestClient
from API.EMInfraRestClient import EMInfraRestClient
from Database.DbManager import DbManager
from Domain.DavieDomain import ZoekTerm
from Domain.EMInfraDomain import FeedProxyPage, ProxyEntryObject


class DeliveryFinder:
    def __init__(self, em_infra_client: EMInfraRestClient, davie_client: DavieRestClient, db_manager: DbManager):
        self.em_infra_client = em_infra_client
        self.davie_client = davie_client
        self.db_manager = db_manager
        self.allowed_dossiers = {'VWT-CEW-2020-009-5'}
        self.filtered_asset_types = {
            'b25kZXJkZWVsI1dWTGljaHRtYXN0',  # 'WVLichtmast'
            'b25kZXJkZWVsI1ZlcmxpY2h0aW5nc3RvZXN0ZWxMRUQ',  # onderdeel#VerlichtingstoestelLED
            'b25kZXJkZWVsI1dWQ29uc29sZQ',  # onderdeel#WVConsole
            'b25kZXJkZWVsI0FybWF0dXVyY29udHJvbGxlcg',  # onderdeel#Armatuurcontroller
            'bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q',  # lgc:installatie#VPLMast
            'bGdjOmluc3RhbGxhdGllI1ZQQ29uc29sZQ'  # lgc:installatie#VPConsole
        }

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
        # find aanlevering in db without davie_uuid or referentie, get them and store them in db
        while True:
            em_infra_uuid = self.db_manager.get_a_delivery_uuid_without_reference()
            if em_infra_uuid is None:
                logging.info('No more deliveries found without reference, continue with getting DAVIE uuid.')
                break
            event_context = self.em_infra_client.get_event_context_by_uuid(uuid=str(em_infra_uuid))
            self.db_manager.update_delivery_description(em_infra_uuid=em_infra_uuid, description=event_context.omschrijving)

        while True:
            delivery_db = self.db_manager.get_a_delivery_without_davie_uuid()
            if delivery_db is None:
                logging.info('No more deliveries found without davie_uuid, continue with getting DAVIE uuid.')
                break
            delivery = self.davie_client.find_delivery_by_search_parameters(
                search_parameters=ZoekTerm(vrijeZoekterm=delivery_db.referentie))
            if delivery.dossierNummer in self.allowed_dossiers:
                self.db_manager.update_delivery_davie_uuid(em_infra_uuid=delivery_db.uuid_em_infra,
                                                           davie_uuid=delivery.id)
                self.db_manager.update_delivery_status(davie_uuid=delivery.id, status=delivery.status)
                logging.info(f'Fetched all attributes of delivery {delivery_db.referentie}.')
            else:
                self.db_manager.delete_delivery_by_uuid(delivery_db.uuid_em_infra)

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
                if any(t in self.filtered_asset_types for t in asset_types):
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

    def get_events_ready_to_process(self, events: list[ProxyEntryObject]):
        # may be used to also add asset uuid and save those to the db as well
        return events

    def save_events_to_process_to_db(self, events_to_process: list[ProxyEntryObject], feed_page_number: str,
                                     event_id: str):
        for event in events_to_process:
            self.db_manager.add_delivery(event.content.value.context_id)

        self.db_manager.set_state_variable('feedproxy_page', feed_page_number)
        self.db_manager.set_state_variable('feedproxy_event_id', event_id)
