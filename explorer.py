import seaborn as sb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import click


def load_data(path: str):
    df = pd.read_json(path, lines=True)
    return df


@click.command()
@click.argument("data_path")
def scatter(data_path):
    data = load_data(data_path)
    price = "total_price"
    data["created_date"] = pd.to_datetime(data['created_date'])
    data[price] = data[price] / 1e18
    filtered = data.query(f"{price} < 10")
    filtered = filtered.query("created_date > 20210923")
    = sb.scatterplot(data=filtered, x="created_date", y=price)

    plt.xticks(rotation=45)
    plt.show(block=True)


@click.group()
def cli():
    pass


cli.add_command(scatter)

if __name__ == "__main__":
    cli()
