import polars as pl
import pandas as pd
from tardis_dev import datasets
from datetime import datetime, timedelta


class TardisLoader:    
    def __init__(self, root_dir: str, api_key: str):
        self._root_dir = root_dir
        self._api_key = api_key

    def download(self, start, end, data_type, exchange, symbol):
        file_name_symbol = symbol.replace("-", "")
        datasets.download(
            exchange=exchange,
            data_types=[data_type],
            from_date=str(start),
            to_date=str(end + timedelta(days=1)),
            symbols=[symbol],
            download_dir=f"{self._root_dir}/{data_type}_{exchange}_{symbol}",
            api_key=self._api_key
        )
    
    def read(self, start, end, data_type, exchange, symbol):
        def path(date: str, data_type: str, exchange: str, symbol: str) -> str:
            return \
                f"{self._root_dir}/{data_type}_{exchange}_{symbol}" \
                f"/{exchange}_{data_type}_{date}_{symbol}.csv.gz"

        dfs = [
            pl.read_csv(path(str(d.date()), data_type, exchange, symbol))
            for d in pd.date_range(start, end)
        ]
        df = pl.concat(dfs).sort("timestamp")

        # Parse the timestamp column as microseconds
        df = df.with_columns([
            pl.col("timestamp").cast(pl.Datetime("us")).alias("human_time")
        ])
        
        # Move 'human_time' to the first column
        df = df.select([
            "human_time",
            *[col for col in df.columns if col != "human_time"]
        ])

        return df