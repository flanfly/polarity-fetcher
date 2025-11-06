from typing import Tuple
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
import time
import logging as l
import os

import pandas as pd
import requests  # type: ignore
import dotenv  # type: ignore
import argparse
from tqdm import tqdm  # type: ignore
from tqdm.contrib.logging import logging_redirect_tqdm  # type: ignore


class Unrecoverable(Exception):
    """
    An error indicating that the operation cannot be retried.
    """

    def __init__(self, message="Unrecoverable error"):
        self.message = message
        super().__init__(self.message)


def available_metrics() -> dict[str, set[str]]:
    resp = requests.get("https://api.polaritydigital.io/api/metrics")
    body = resp.json()

    if resp.status_code != requests.codes.ok:
        raise Exception(
            "Error fetching metrics: %s" % body.get("message", "Unknown error")
        )
    if "status" in body and body["status"] != 1:
        raise Exception(
            "Error fetching metrics: %s" % body.get("message", "Unknown error")
        )

    data = body["data"]

    ret: dict[str, set[str]] = {}
    for metric in data["allDashboardMetrics"]:
        if not metric.get("show_on_workbench", False):
            continue
        for coin in metric.get("coins", []):
            ret.setdefault(coin, set()).add(metric["key"])

    return ret


def get_data(asset: str, metric: str, idtoken: str):
    headers = {"authorization": "Bearer " + idtoken}
    params = {"coin": asset, "metric": metric}

    resp = requests.get(
        "https://api.polaritydigital.io/api/historicalData",
        headers=headers,
        params=params,
        timeout=30,
    )

    if resp.status_code >= 400 and resp.status_code < 500:
        raise Unrecoverable(
            f"Client error {resp.status_code}: {resp.text}. Check your ID token."
        )
    if resp.status_code != requests.codes.ok:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
    body = resp.json()
    if "status" in body and body["status"] != 1:
        raise Exception(
            "Error fetching data: %s" % body.get("message", "Unknown error")
        )

    timestamps = []
    values = []
    for row in body["data"]:
        timestamps.append(pd.to_datetime(row["closetime"]))
        values.append(row["closeprice"])

    return pd.DataFrame({"timestamp": timestamps, metric: values})


def do_work(item: Tuple[str, str], idtoken: str):
    coin, metric = item
    asset = coin.lower()

    l.info(f"fetching {asset} {metric}...")

    wait = 0
    while True:
        try:
            df = get_data(asset, metric, idtoken)
            df["asset"] = asset
            df.set_index(["timestamp", "asset"], inplace=True)

            return df

        except Unrecoverable as e:
            l.error(f"unrecoverable error fetching {asset} {metric}: {e}")
            return pd.DataFrame()

        except Exception as e:
            l.error(f"error fetching {asset} {metric}: {e}")
            w = min(2**wait, 30)
            l.info(f"waiting {w} seconds before retrying...")
            time.sleep(w)
            wait += 1


def main():
    dotenv.load_dotenv()
    l.basicConfig(level=l.INFO)

    parser = argparse.ArgumentParser(
        description="Update Polarity Digital metrics and store in HDF5 file"
    )
    parser.add_argument(
        "--idtoken",
        type=str,
        default=os.environ.get("POLARITY_IDTOKEN", ""),
        help="Polarity Digital ID token (or set POLARITY_IDTOKEN env variable)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="polarity-digital.parquet",
        help="Output Parquet file",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=4,
        help="Number of parallel requests to make",
    )
    args = parser.parse_args()

    with logging_redirect_tqdm():
        l.info("fetching available metrics...")
        metrics_by_coin = available_metrics()

        l.info(f"writing to {args.output}")
        l.info(f"{len(metrics_by_coin)} coins found")

        with ThreadPoolExecutor(max_workers=args.parallelism) as executor:
            items = [(c, m) for c, ms in metrics_by_coin.items() for m in ms]
            gen = executor.map(lambda c: do_work(c, args.idtoken), items)

            df = None
            for dff in tqdm(gen, total=len(items), desc="fetching data"):
                try:
                    if len(dff) == 0:
                        continue
                    if df is None:
                        df = dff
                    else:
                        df = pd.merge(
                            df, dff, left_index=True, right_index=True, how="outer"
                        )
                except Exception as e:
                    l.error(f"error merging data: {e}")
                    continue

        l.info("storing data...")
        df.to_parquet(args.output, engine="pyarrow", compression="snappy")


if __name__ == "__main__":
    main()
