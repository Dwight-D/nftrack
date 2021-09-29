from datetime import datetime
from requests import Response
from typing import Optional

import json
import requests


def get(path) -> Response:
    return requests.get(f"{path}")


class OpenseaConfig:
    api_root: str = "https://api.opensea.io/api"
    events_api: str = f"{api_root}/v1/events"
    collections_api: str = f"{api_root}/v1/collections"


class ApiClient:
    config: OpenseaConfig

    def __init__(self, config: OpenseaConfig):
        self.config = config


    def get_collection_data(self, collection: str):
        path = f"{self.config.collections_api}"


    def get_collection_events(self,
                              collection: str,
                              limit: int,
                              offset: int,
                              after_time: datetime,
                              event_type: Optional[str] = None):
        """
        Makes a request to the Events API and returns the events for the given query

        :param collection: The collection for which to fetch events
        :param limit: How many events to fetch in the batch
        :param offset: Pagination offset
        :param after_time: Fetch events after this point in time
        :param event_type: Optional - The type of event to fetch. See https://docs.opensea.io/reference/retrieving-asset-events for supported event types. Defaults to all
        :return: A list of events
        """

        path = f"{self.config.events_api}" \
               f"?collection_slug={collection}" \
               f"&occurred_after={after_time.timestamp()}" \
               f"&limit={limit}" \
               f"&offset={offset}" \
               f'{("&event_type=" + event_type) if event_type else ""}'
        return get(path)

    def yield_all_events(self,
                         collection: str,
                         after_time: datetime,
                         event_type: Optional[str] = None):
        """
        Generator that yields batches of events by paginating the API until all events have been consumed

        :param collection: The collection for which to fetch events
        :param after_time: Fetch events after this point in time
        :param event_type: Optional - The type of event to fetch. See https://docs.opensea.io/reference/retrieving-asset-events for supported event types. Defaults to all
        :return: Paginated lists of events
        """

        limit = 300
        offset = 0
        while True:
            print(f"Loading data, offset: {offset}")
            response = self.get_collection_events(collection, limit, offset, after_time, event_type)
            if response.ok:
                events = json.loads(response.content)['asset_events']
                if events:
                    fetched_count = len(events)
                    print(f"Got {fetched_count} in batch")
                    offset = offset + fetched_count
                    yield events
                else:
                    return False
            else:
                print(f"Error while fetching data. Response: {response.status_code}")
                print(response.text)
                print(response.reason)
