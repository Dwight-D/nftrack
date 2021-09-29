import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import List

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteApi
from influxdb_client.client.write_api import SYNCHRONOUS

from model import OpenseaEvent


def to_line(event: OpenseaEvent):
    return f"{event.type},collection={event.collection},platform=opensea price={event.price_wei},event_id={event.id} {int(event.time.timestamp() * 1000000000)}",


class InfluxConfig:
    token: str
    token: str
    organization: str
    bucket: str
    url: str = "https://westeurope-1.azure.cloud2.influxdata.com"

    def __init__(self):
        load_dotenv()
        self.token = os.getenv('INFLUX_TOKEN')
        self.organization = os.getenv('INFLUX_ORGANIZATION')
        self.bucket = os.getenv('INFLUX_BUCKET')
        self.url = os.getenv('INFLUX_URL')


class InfluxClient:
    config: InfluxConfig
    client: InfluxDBClient
    write_api: WriteApi

    def __init__(self, config: InfluxConfig):
        self.config = config
        self.client = InfluxDBClient(
            url=config.url,
            token=config.token,
            org=config.organization
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write_point(self, point: Point):
        self.write_api.write(self.config.bucket, self.config.organization, point)

    def read_data(self, window_minutes: int, collection: str):
        query = f'from(bucket: "{self.config.bucket}")' \
                f'|> range(start: -{window_minutes}m)' \
                f'|> filter(fn: (r) => r["collection"] == "{collection}")'
        return self.client.query_api().query(query, org=self.config.organization)

    def write_lines(self, lines: List[str]):
        self.write_api.write(
            bucket=self.config.bucket,
            org=self.config.organization,
            record=lines,
            write_precision=WritePrecision.NS
        )

    def write_events(self, events: List[OpenseaEvent]):
        self.write_lines([to_line(e) for e in events])

    def seed(self):
        n = [1, 3, 5]
        events = []
        for i in n:
            events.append(OpenseaEvent(
                time=datetime.now() - timedelta(minutes=i),
                price_wei=int(1e18 + (i * 1e17)),
                id=i,
                type=OpenseaEvent.EventType.SALE,
                collection="test-collection"
            ))
        self.write_events(events)


def main():
    config = InfluxConfig()
    client = InfluxClient(config)
    print("Done")


if __name__ == "__main__":
    main()
