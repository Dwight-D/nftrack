import seaborn as sb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import click


def load_data(path: str):
    df = pd.read_json(path, lines=True)
    return df


def plot_stuff():
    successful = load_data("./data/filtered/successful.json")
    created = load_data("./data/filtered/created.json")
    successful["created_date"] = pd.to_datetime(successful['created_date'])
    successful["total_price"] = successful["total_price"] / 1e18

    created["created_date"] = pd.to_datetime(created['created_date'])
    created["ending_price"] = created["ending_price"] / 1e18

    successful = successful.query("total_price < 5")
    successful = successful.query("created_date > 20210923")

    created = created.query("ending_price < 10")
    created = created.query("created_date > 20210923")
    
    sb.scatterplot(data=successful, x="created_date", y="total_price")
    #sb.scatterplot(data=created, x="created_date", y="ending_price")

    plt.xticks(rotation=45)
    plt.show(block=True)


@click.command()
@click.argument("data_path")
def scatter(data_path):
    plot_stuff()


@click.group()
def cli():
    pass


cli.add_command(scatter)

if __name__ == "__main__":
    cli()
