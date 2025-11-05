import psycopg  # type: ignore
import pandas as pd
import os
import dotenv  # type: ignore
import logging as l
from tqdm.contrib.logging import logging_redirect_tqdm  # type: ignore
import argparse


def migrate(cur):
    cur.execute("create extension if not exists timescaledb;")
    cur.execute("select to_regclass('metrics') is not null;")
    if cur.fetchone()[0]:
        return

    cur.execute("drop table if exists metrics;")
    cur.execute("drop table if exists assets;")
    cur.execute(
        """
create table assets (
  id serial primary key,
  symbol text not null
);
            """
    )
    cur.execute("create unique index ix_asset_symbol on assets (symbol);")
    cur.execute(
        """
create table metrics (
  time timestamptz not null,
  asset_id integer not null references assets(id),
  closeprice double precision not null,
  udpil double precision not null,
  udpis double precision not null,
  udpim double precision not null,
  mbi double precision not null,
  tci double precision not null,
  tcicv double precision not null,
  mdccv double precision not null,
  upprob double precision not null,
  mtm double precision null,
  mcm double precision null
);
            """
    )
    cur.execute("select create_hypertable('metrics', by_range('time'));")
    cur.execute(
        "create unique index ix_metrics_asset_time on metrics (asset_id, time desc);"
    )


def load_dataframe(cur, df, symbol: str):
    # insert asset row
    cur.execute(
        "insert into assets (symbol) values (%s) on conflict do nothing",
        (symbol,),
    )
    cur.execute("select id from assets where symbol = %s", (symbol,))
    asset_id = cur.fetchone()[0]

    # fetch latest metric's timestamp
    cur.execute(
        "select time from metrics where asset_id = %s order by time desc limit 1",
        (asset_id,),
    )
    row = cur.fetchone()

    # delete that as it may have changed
    cur.execute(
        "delete from metrics where asset_id = %s and time = %s", (asset_id, row)
    )

    # copy new rows
    with cur.copy(
        "copy metrics (time, asset_id, closeprice, udpil, udpis, udpim, mbi, tci, tcicv, mdccv, upprob, mtm, mcm) from stdin"
    ) as copy:
        if row is None:
            newrows = df
        else:
            ts = pd.to_datetime(row[0]).to_datetime64()
            newrows = df.loc[df.index >= ts]
        for idx, row in newrows.iterrows():
            copy.write_row(
                (
                    idx.to_pydatetime(),
                    asset_id,
                    row["closeprice"],
                    row["udpil"],
                    row["udpis"],
                    row["udpim"],
                    row["mbi"],
                    row["tci"],
                    row["tcicv"],
                    row["mdccv"],
                    row["upprob"],
                    row.get("mtm", None),
                    row.get("mcm", None),
                )
            )


def main():
    dotenv.load_dotenv()
    l.basicConfig(level=l.INFO)

    parser = argparse.ArgumentParser(
        description="Load Polarity Digital metrics from Parquet file into PostgreSQL database"
    )
    parser.add_argument(
        "--postgres-uri",
        type=str,
        default=os.environ.get("POSTGRES_DSN", "host=localhost dbname=polarity"),
        help="PostgreSQL connection string (or set POSTGRES_DSN env variable)",
    )
    parser.add_argument(
        "--input",
        type=str,
        default="polarity-digital.parquet",
        help="Input Parquet file",
    )
    args = parser.parse_args()

    l.info("loading Parquet file...")
    df = pd.read_parquet(args.input)

    with logging_redirect_tqdm():
        # Connect to an existing database
        with psycopg.connect(args.postgres_uri) as conn:
            with conn.cursor() as cur:
                migrate(cur)
                conn.commit()

            assets = df.index.get_level_values("asset").unique()
            for asset in assets:
                l.info(f"loading data for {asset}...")
                df_asset = df.xs(asset, level="asset")
                with conn.cursor() as cur:
                    cur.execute("set timezone to 'UTC';")
                    load_dataframe(cur, df_asset, asset)
                    conn.commit()


if __name__ == "__main__":
    main()
