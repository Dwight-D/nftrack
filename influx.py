import os
from dotenv import load_dotenv
from dataclasses import dataclass

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteApi
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import List

from model import OpenseaEvent


def to_line(event: OpenseaEvent):
    return f"{event.type.value},collection={event.collection},platform=opensea price={event.price_wei} {int(event.time.timestamp() * 1000000000)}",


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

    def read_data(self, window_minutes: int):
        query = f'from(bucket: "{self.config.bucket}") |> range(start: -{window_minutes}m)'
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


def main():
    config = InfluxConfig()
    client = InfluxClient(config)
    #client.read_data(6)
    print("Done")


if __name__ == "__main__":
    main()
