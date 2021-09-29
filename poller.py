import json
import os
import click
from datetime import datetime, timedelta, timezone
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
            return t - timedelta(seconds=1)
        else:
            print(f"No sample found within lookback window, polling from {self.config.lookback_window_max_minutes} minutes ago")
            return datetime.now() - timedelta(minutes=self.config.lookback_window_max_minutes)

    def poll_events(self, collections: List[str], start_time: datetime = None):
        """
        Fetches events for the specified collections and pushes them to InfluxDB
        :param collections: the collections for which to query data
        :param start_time: OPTIONAL - The start of the query range. If not passed it will be set automatically from config lookback window param
        :return:
        """
        for collection in collections:
            if not start_time:
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

    def poll_collections(self, collections: List[str], start_time: datetime = None):
        """
        Fetches collection data for the specified collections and pushes them to InfluxDB
        :param collections: the collections for which to query data
        :param start_time: OPTIONAL - The start of the query range. If not passed it will be set automatically from config lookback window param
        :return:
        """
        print(f"Polling {collections} collection data")
        collection_data = self.opensea_client.get_collection_data(
            collections=collections,
        )
        with open("collections.data", "w") as f:
            f.writelines(json.dumps(collection_data))
        print("Done")


@click.group(name="bootstrap")
def group_bootstrap():
    pass


@click.command(name="events")
@click.argument("collection")
def bootstrap_events(collection):
    config = PollerConfig()
    poller = Poller(config)
    start_time = datetime.now(tz=timezone.utc) - timedelta(minutes=config.bootstrap_window_minutes)
    click.echo(f"Bootstrapping {collection} events, starting from {start_time}")
    poller.poll_events([collection], start_time)


@click.group(name="poll")
def group_poll():
    pass


@click.command(name="collection")
@click.argument("collection")
def poll_collections(collection):
    config = PollerConfig()
    poller = Poller(config)
    click.echo(f"Bootstrapping {config.collections} collection")
    poller.poll_collections(config.collections)


@click.command(name="events")
def poll_events():
    config = PollerConfig()
    poller = Poller(config)
    click.echo(f"Polling {config.collections}")
    poller.poll_events(config.collections)


@click.group()
def cli():
    pass


group_bootstrap.add_command(bootstrap_events)
group_poll.add_command(poll_events)
group_poll.add_command(poll_collections)
cli.add_command(group_poll)
cli.add_command(group_bootstrap)


if __name__ == "__main__":
    cli()
