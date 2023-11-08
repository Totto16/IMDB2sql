import gzip
import os
import shutil
from multiprocessing import Pool, cpu_count
from os.path import exists
from typing import List

import requests
from rich.progress import track
from tqdm.auto import tqdm

from src.utils import DataSet


class DataSetsHandler:
    def __init__(self, data_sets: List[DataSet]) -> None:
        self.data_sets = data_sets

    def download(self):
        print("Downloading ...")
        for item in self.data_sets:
            self._download_file(data_set=item)

    @staticmethod
    def _download_file(data_set: DataSet) -> None:
        with requests.get(data_set.url, stream=True) as r:
            total_length = int(r.headers.get("Content-Length") or "-1")

            if total_length < 0:
                raise RuntimeError("Negative Content Length")

            with tqdm.wrapattr(
                r.raw,
                "read",
                total=total_length,
                desc=f"{data_set.url} -> {data_set.gzipped} ...",
            ) as raw:
                with open(data_set.gzipped, "wb") as output:
                    shutil.copyfileobj(raw, output)

    def extract(self) -> None:
        print("Extracting ...")
        with Pool(cpu_count()) as pool:
            pool.map(self._extract_file, self.data_sets)

    @staticmethod
    def _extract_file(data_set: DataSet) -> None:
        print(f"{data_set.gzipped} -> {data_set.extracted} ...")
        with gzip.open(data_set.gzipped) as zf:
            with open(data_set.extracted, "w") as f:
                for line in track(zf, description="[green]Extracting dataset ..."):
                    f.write(line.decode())

    def cleanup(self) -> None:
        for data_set in self.data_sets:
            if exists(data_set.gzipped):
                os.remove(data_set.gzipped)

            if exists(data_set.extracted):
                os.remove(data_set.extracted)
