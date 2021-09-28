import os
from datetime import datetime, timedelta
from typing import List

from dotenv import load_dotenv

from influx import InfluxConfig, InfluxClient
from opensea import ApiClient as OpenseaClient, OpenseaConfig
from model import OpenseaEvent, event_from_dict


class PollerConfig:
    collections: List[str]
    lookback_window_max_minutes: int
    bootstrap_window_minutes: int
    event_types: List[str]

    def __init__(self):
        load_dotenv()
        collection_string = os.getenv('POLL_COLLECTIONS')
        self.collections = collection_string.split(",")
        types_string = os.getenv('POLL_EVENT_TYPES')
        self.event_types = collection_string.split(",")
        self.lookback_window_max_minutes = int(os.getenv('POLL_LOOKBACK_WINDOW_MAX_MINUTES'))
        self.lookback_window_initial_minutes = int(os.getenv('POLL_LOOKBACK_WINDOW_INITIAL_MINUTES'))
        self.bootstrap_window_minutes = int(os.getenv('POLL_BOOTSTRAP_WINDOW_MINUTES'))


class Poller:
    config: PollerConfig
    influx_client: InfluxClient
    opensea_client: OpenseaClient

    def __init__(self, cfg):
        self.config = cfg
        influx_config = InfluxConfig()
        self.influx_client = InfluxClient(influx_config)

        opensea_config = OpenseaConfig()
        self.opensea_client = OpenseaClient(OpenseaConfig)

    def find_window_start(self, collection: str) -> datetime:
        lookback_minutes = self.config.lookback_window_initial_minutes
        previous_data = []

        # Loop with exponential backoff until data is found or max lookback threshold is surpassed
        while not previous_data and lookback_minutes < self.config.lookback_window_max_minutes:
            previous_data = self.influx_client.read_data(
                window_minutes=lookback_minutes,
                collection=collection
            )
            lookback_minutes = lookback_minutes * 2
        if previous_data and previous_data[0].records:
            records = previous_data[0].records
            records.sort(
                key=lambda record: record.values["_time"],
                reverse=True
            )
            last_sample = records[0]
            t = last_sample.values["_time"]
            print(f"Found last sample at {t}")
            return t
        else:
            print(f"No sample found within lookback window, polling from {self.config.lookback_window_max_minutes} minutes ago")
            return datetime.now() - timedelta(minutes=self.config.lookback_window_max_minutes)

    def poll(self):
        for collection in self.config.collections:
            start_time = self.find_window_start(collection=collection)
            print(f"Polling {collection} events from {start_time}")
            event_batches = self.opensea_client.yield_all_events(
                collection=collection,
                after_time=start_time,
                event_type="successful"
            )
            for batch in event_batches:
                events = [event_from_dict(d) for d in batch]
                self.influx_client.write_events(events)



def main():
    config = PollerConfig()
    poller = Poller(config)
    poller.poll()


if __name__ == "__main__":
    main()
