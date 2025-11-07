from typing import Tuple
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
import time
import logging as l
import os
import unittest

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
        raise Unrecoverable(f"Client error {resp.status_code}: {resp.text}.")
    if resp.status_code != requests.codes.ok:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
    body = resp.json()
    if "status" in body and body["status"] != 1:
        raise Exception(
            "Error fetching data: %s" % body.get("message", "Unknown error")
        )

    l.debug(f"response body: {body}")

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
            l.debug(f"fetched {len(df)} rows for {asset} {metric}")
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


class TestDoMerge(unittest.TestCase):
    def test_update_column(self):
        df1 = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-02",
                        "2023-01-03",
                        "2023-01-01",
                        "2023-01-02",
                        "2023-01-03",
                    ]
                ),
                "asset": ["btc", "btc", "btc", "eth", "eth", "eth"],
                "metric1": [1.0, 2.0, 3.0, None, None, None],
                "metric2": [None, None, None, 1.0, 2.0, 3.0],
            }
        ).set_index(["timestamp", "asset"])

        df2 = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                "asset": ["btc", "btc", "btc"],
                "metric2": [4.0, 5.0, 6.0],
            }
        ).set_index(["timestamp", "asset"])

        merged = do_merge(df1, df2)
        expected = pd.DataFrame(
            {
                "metric1": [1.0, 2.0, 3.0, None, None, None],
                "metric2": [4.0, 5.0, 6.0, 1.0, 2.0, 3.0],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    (pd.to_datetime("2023-01-01"), "btc"),
                    (pd.to_datetime("2023-01-02"), "btc"),
                    (pd.to_datetime("2023-01-03"), "btc"),
                    (pd.to_datetime("2023-01-01"), "eth"),
                    (pd.to_datetime("2023-01-02"), "eth"),
                    (pd.to_datetime("2023-01-03"), "eth"),
                ],
                names=["timestamp", "asset"],
            ),
        ).sort_index()
        pd.testing.assert_frame_equal(merged, expected)

    def test_new_column(self):
        df1 = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                "asset": ["btc", "btc", "btc"],
                "metric1": [1.0, 2.0, 3.0],
            }
        ).set_index(["timestamp", "asset"])

        df2 = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                "asset": ["btc", "btc", "btc"],
                "metric2": [4.0, 5.0, 6.0],
            }
        ).set_index(["timestamp", "asset"])

        merged = do_merge(df1, df2)

        expected = pd.DataFrame(
            {
                "metric1": [1.0, 2.0, 3.0],
                "metric2": [4.0, 5.0, 6.0],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    (pd.to_datetime("2023-01-01"), "btc"),
                    (pd.to_datetime("2023-01-02"), "btc"),
                    (pd.to_datetime("2023-01-03"), "btc"),
                ],
                names=["timestamp", "asset"],
            ),
        ).sort_index()

        pd.testing.assert_frame_equal(merged, expected)

    def test_new_asset(self):
        df1 = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                "asset": ["btc", "btc", "btc"],
                "metric1": [1.0, 2.0, 3.0],
            }
        ).set_index(["timestamp", "asset"])

        df2 = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                "asset": ["eth", "eth", "eth"],
                "metric2": [4.0, 5.0, 6.0],
            }
        ).set_index(["timestamp", "asset"])

        merged = do_merge(df1, df2)

        expected = pd.DataFrame(
            {
                "metric1": [1.0, 2.0, 3.0, None, None, None],
                "metric2": [None, None, None, 4.0, 5.0, 6.0],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    (pd.to_datetime("2023-01-01"), "btc"),
                    (pd.to_datetime("2023-01-02"), "btc"),
                    (pd.to_datetime("2023-01-03"), "btc"),
                    (pd.to_datetime("2023-01-01"), "eth"),
                    (pd.to_datetime("2023-01-02"), "eth"),
                    (pd.to_datetime("2023-01-03"), "eth"),
                ],
                names=["timestamp", "asset"],
            ),
        ).sort_index()

        pd.testing.assert_frame_equal(merged, expected)


def do_merge(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    adf1, adf2 = df1.align(df2, join="outer", axis=0, copy=False)

    for col in df2.columns:
        if col not in adf1.columns:
            adf1[col] = adf2[col]
        else:
            adf1[col] = adf2[col].combine_first(adf1[col])

    return adf1.sort_index()


def main():
    dotenv.load_dotenv()

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
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="File to write logs to (if not set, logs to stdout)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    parser.add_argument(
        "--flush-interval",
        type=int,
        default=100,
        help="Number of metrics to fetch before flushing intermediate results to disk",
    )
    args = parser.parse_args()

    l.basicConfig(
        level=getattr(l, args.log_level.upper(), l.INFO),
        filename=args.log_file,
    )
    output_file = args.output.replace(":", "_")
    temp_file = f"{output_file}.part"

    with logging_redirect_tqdm():
        l.info("fetching available metrics...")
        metrics_by_coin = available_metrics()

        l.info(f"writing to {output_file}")
        l.info(f"{len(metrics_by_coin)} coins found")

        flush_countdown = args.flush_interval
        df = None

        with ThreadPoolExecutor(max_workers=args.parallelism) as executor:
            items = [(c, m) for c, ms in metrics_by_coin.items() for m in ms]
            gen = executor.map(lambda c: do_work(c, args.idtoken), items)

            flush_countdown = args.flush_interval

            for df2 in tqdm(gen, total=len(items), desc="fetching data"):
                try:
                    if df2.empty:
                        continue

                    # merge dataframes
                    if df is None:
                        df = df2
                    else:
                        df = do_merge(df, df2)

                    # periodic flush to disk
                    flush_countdown -= 1
                    if flush_countdown <= 0:
                        l.info("intermediate storing data...")
                        df.to_parquet(temp_file, engine="pyarrow", compression="snappy")
                        flush_countdown = args.flush_interval

                except Exception as e:
                    l.error(f"error merging data: {e}")
                    continue

        l.info("storing data...")
        df.to_parquet(output_file, engine="pyarrow", compression="snappy")

        os.remove(temp_file)


if __name__ == "__main__":
    main()
