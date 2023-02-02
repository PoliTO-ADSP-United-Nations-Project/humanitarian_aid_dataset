"""This module defines abstract structure of a dataset"""

from abc import ABC, abstractmethod

import pandas as pd

from base.Pipeline import Pipeline


class Dataset(ABC):

    def __init__(self, name: str, pipeline: Pipeline = None, verbose: bool = False) -> None:
        self._name = name
        self._size = 0
        self._verbose = verbose
        self._pipeline = pipeline
        # Here you've the possibility to define a single dataset or a list of datasets
        self._dataset = None
        self._multiple_datasets = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def size(self) -> int:
        return self._size

    @size.setter
    def size(self, value):
        self._size = value

    @property
    def verbose(self) -> bool:
        return self._verbose

    @verbose.setter
    def verbose(self, value):
        self._verbose = value

    @property
    def dataset(self) -> pd.DataFrame:
        return self._dataset

    @dataset.setter
    def dataset(self, value: pd.DataFrame):
        self._dataset = value
        self.size = len(self._dataset)

    def add_multiple_dataset(self, df: pd.DataFrame):
        self._multiple_datasets.append((df, len(df)))

    def get_multiple_dataset(self, idx: int) -> pd.DataFrame:
        if idx < len(self._multiple_datasets):
            return self._multiple_datasets[idx][0]
        raise ValueError(f'Index too big: max size is {len(self._multiple_datasets)}')

    def get_size_multiple_dataset(self, idx: int) -> int:
        if idx < len(self._multiple_datasets):
            return self._multiple_datasets[idx][1]
        raise ValueError(f'Index too big: max size is {len(self._multiple_datasets)}')

    def remove_all_datasets(self):
        self._multiple_datasets = []

    def set_pipeline(self, pipeline: Pipeline):
        self._pipeline = pipeline

    @abstractmethod
    def download(self, path: str) -> None:
        pass

    def do_pipeline(self, path: str = None, download_first: bool = True) -> None:
        if self._pipeline is None:
            print(f'No pipeline for {self.name} dataset')
            return
        if download_first and path is not None:
            print(f'==STARTING DOWNLOAD "{self.name}" DATASET==')
            self.download(path)
        self._pipeline.do_pipeline(path)

    def print(self, text: str) -> None:
        if self._verbose:
            print(text)
