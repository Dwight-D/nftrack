import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from influx import InfluxConfig, InfluxClient
from model import OpenseaEvent


class PollerConfig:
    collection: str
    lookback_window_max_minutes: int
    bootstrap_window_minutes: int

    def __init__(self):
        load_dotenv()
        self.collection = os.getenv('POLL_COLLECTION')
        self.lookback_window_max_minutes = int(os.getenv('POLL_LOOKBACK_WINDOW_MAX_MINUTES'))
        self.lookback_window_initial_minutes = int(os.getenv('POLL_LOOKBACK_WINDOW_INITIAL_MINUTES'))
        self.bootstrap_window_minutes = int(os.getenv('POLL_BOOTSTRAP_WINDOW_MINUTES'))


class Poller:
    config: PollerConfig
    influx_client: InfluxClient

    def __init__(self, cfg):
        self.config = cfg
        influx_config = InfluxConfig()
        self.influx_client = InfluxClient(influx_config)

    def find_window_start(self) -> datetime:
        lookback_minutes = self.config.lookback_window_initial_minutes
        previous_data = []

        # Loop with exponential backoff until data is found or max lookback threshold is surpassed
        while not previous_data and lookback_minutes < self.config.lookback_window_max_minutes:
            previous_data = self.influx_client.read_data(lookback_minutes)
            lookback_minutes = lookback_minutes * 2
        if previous_data:
            # TODO: find last sample
            last_sample = None
            return datetime.now()
        else:
            return datetime.now() - timedelta(minutes=self.config.lookback_window_max_minutes)

    def seed(self):
        n = [10, 60, 800]
        events = []
        for i in n:
            events.append(OpenseaEvent(
                time=datetime.now() - timedelta(minutes=i),
                price_wei=1e18 + (i * 1e17),
                id=i,
                type=OpenseaEvent.EventType.SALE,
                collection="test-collection"
            ))
        self.influx_client.write_events(events)


def main():
    config = PollerConfig()
    poller = Poller(config)
    poller.seed()

    # window_start = poller.find_window_start()


if __name__ == "__main__":
    main()
