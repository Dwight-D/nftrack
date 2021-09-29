from datetime import datetime
from requests import Response
from typing import Optional, List

import json
import time
import requests


def get(path) -> Response:
    return requests.get(f"{path}")


def get_error(response: Response):
    print(f"Error while fetching data. Response: {response.status_code}")
    print(response.text)
    print(response.reason)
    return False


class OpenseaConfig:
    api_root: str = "https://api.opensea.io/api"
    events_api: str = f"{api_root}/v1/events"
    collections_api: str = f"{api_root}/v1/collections"


class ApiClient:
    config: OpenseaConfig

    def __init__(self, config: OpenseaConfig):
        self.config = config

    def paginate_all(self, request_function):
        pass

    def get_collection_data(self, collections: List[str]):
        output = []
        searching = collections.copy()
        offset = 0
        limit = 300
        backoff = 1
        max_backoff = 60
        with open("collections.names", "w") as f:
            while searching:
                path = f"{self.config.collections_api}" \
                       f"?limit={limit}" \
                       f"&offset={offset}"
                response = get(path)
                if not response.ok:
                    # Retry and backoff on server error or forbidden/throttling, might be temporary
                    if response.status_code == 500 or response.status_code == 403:
                        print(f"{response.status_code} response, backing off and retrying")
                        if backoff > max_backoff:
                            print("Backoff hit upper limit, giving up")
                            return output
                        time.sleep(backoff)
                        backoff = backoff * 2
                        continue
                    # Print trace info for unexpected errors
                    return get_error(response)
                data = json.loads(response.text)["collections"]
                if not data:
                    print(f"Warning: No more collection data found, still searching for {searching}")
                    print(response.text)
                    break
                collections_count = len(data)
                offset = offset + collections_count
                print(f"Loaded {collections_count} more collections, total: {offset}")
                for c in data:
                    f.write(c["slug"])
                    f.write("\n")
                    if c["slug"] in collections:
                        output.append(c)
                        searching.remove(c["slug"])
        return output

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
            if not response.ok:
                return get_error(response)
            events = json.loads(response.content)['asset_events']
            if events:
                fetched_count = len(events)
                print(f"Got {fetched_count} in batch")
                offset = offset + fetched_count
                yield events
            else:
                return False
