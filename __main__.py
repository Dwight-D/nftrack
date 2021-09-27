import simplejson as json
import datetime as dt
import os
import click

from pathlib import Path

from opensea import ApiClient, OpenseaConfig


def clean_file_ending(path):
    """
    Remove the last two bytes of the file to remove trailing comma and newline to conform to JSON spec
    :param path:
    :return: None
    """
    with open(path, 'rb+') as f:
        f.seek(-2, os.SEEK_END)
        f.truncate()


def data_dir(collection: str):
    p = Path(f"./data/{collection}")
    p.mkdir(parents=True, exist_ok=True)
    return p.absolute()


@click.group()
def download():
    pass


@click.command("events")
@click.argument("collection", type=click.STRING)
@click.argument("event_type", required=False)
def download_events(collection, event_type):
    out_file = f'{data_dir(collection)}/{event_type if event_type else "all"}.events'
    click.echo(f'Fetching {event_type if event_type else "all"} events for {collection}. Writing to {out_file}')
    config = OpenseaConfig()
    client = ApiClient(config)
    start_time = dt.datetime.now() - dt.timedelta(days=14)
    event_batches = client.yield_all_events(collection=collection, after_time=start_time, event_type=event_type)
    event_count = 0
    with open(out_file, "w") as file:
        for batch in event_batches:
            batch_size = len(batch)
            event_count = event_count + batch_size
            for idx, event in enumerate(batch):
                file.writelines(json.dumps(event))
                file.writelines(",\n")
    clean_file_ending(out_file)

    click.echo(f"Done, got {event_count} events")


@click.group()
def cli():
    pass


download.add_command(download_events)
cli.add_command(download)

if __name__ == "__main__":
    cli()
