"""This module defines how to build the Climate dataset
* URL: (https://climateknowledgeportal.worldbank.org/)
* NAME: CLIMATE CHANGE KNOWLEDGE PORTAL - WORLD BANK GROUP
"""

import os.path
from typing import Dict

import pandas as pd
import requests

from base.Dataset import Dataset
from constant.AvaiableCountries import COUNTRIES
from constant.DatasetUrl import CLIMATE_CHANGE_PORTAL_URL
from base.Pipeline import Pipeline
from pipeline.impl.EmptyPipeline import EmptyPipeline
from utils.FileUtils import FileUtils


def _check_file_already_exists(country, path) -> bool:
    return FileUtils.has_file(str(country) + '*.csv', path)


class ClimateDataset(Dataset):

    def __init__(self, pipeline: Pipeline = None, verbose: bool = False) -> None:
        if pipeline is None:
            # Default pipeline
            pipeline = EmptyPipeline()
        # Params needed to download datasets
        self._csv_columns = ['Year', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        self._headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "referer": "https://climateknowledgeportal.worldbank.org/download-data",
            "origin": "https://climateknowledgeportal.worldbank.org"
        }
        self._base_http_request = {
            'collection': 'cru',
            'aggregation': 'monthly',
            'type': 'country',
            'timeperiod': 'historical',
            'model': 'all',
            'tab': 'timeseries'
        }
        # Constants
        self.MAX_TEMP_FOLDER_NAME = "max_temp"
        self.MIN_TEMP_FOLDER_NAME = "min_temp"
        self.AVG_TEMP_FOLDER_NAME = "avg_temp"
        self.MM_PREC_FOLDER_NAME = "mm_prec"

        super().__init__("Climate dataset", pipeline=pipeline, verbose=verbose)

    def download(self, path: str) -> None:
        MAX_TEMP_FOLDER = os.path.join(path, self.MAX_TEMP_FOLDER_NAME)
        MIN_TEMP_FOLDER = os.path.join(path, self.MIN_TEMP_FOLDER_NAME)
        AVG_TEMP_FOLDER = os.path.join(path, self.AVG_TEMP_FOLDER_NAME)
        MM_PREC_FOLDER = os.path.join(path, self.MM_PREC_FOLDER_NAME)
        if not os.path.exists(MAX_TEMP_FOLDER):
            os.makedirs(MAX_TEMP_FOLDER)
        if not os.path.exists(MIN_TEMP_FOLDER):
            os.makedirs(MIN_TEMP_FOLDER)
        if not os.path.exists(AVG_TEMP_FOLDER):
            os.makedirs(AVG_TEMP_FOLDER)
        if not os.path.exists(MM_PREC_FOLDER):
            os.makedirs(MM_PREC_FOLDER)

        # Loop for each avaiable countries and download it
        for country in COUNTRIES:
            if _check_file_already_exists(country, MAX_TEMP_FOLDER) and _check_file_already_exists(country,
                                                                                                   MIN_TEMP_FOLDER) \
                    and _check_file_already_exists(country, AVG_TEMP_FOLDER) and _check_file_already_exists(country,
                                                                                                            MM_PREC_FOLDER):
                self.print(f'Country {country} already exists')
                # TODO: aggiornare download_counter
                continue

            self._base_http_request['country'] = country

            self.print(f'AVG-TEMP: Downloading {country}...')
            self._base_http_request['variable'] = 'tas'
            df_avg_temp = self._make_http_request(self._base_http_request)
            self.print(f'MAX-TEMP: Downloading {country}...')
            self._base_http_request['variable'] = 'tasmax'
            df_max_temp = self._make_http_request(self._base_http_request)
            self.print(f'MIN-TEMP: Downloading {country}...')
            self._base_http_request['variable'] = 'tasmax'
            df_min_temp = self._make_http_request(self._base_http_request)
            self.print(f'MM-PREC: Downloading {country}...')
            self._base_http_request['variable'] = 'pr'
            df_mm_prec = self._make_http_request(self._base_http_request)

            if df_avg_temp is not None and df_max_temp is not None and df_min_temp is not None and df_mm_prec is not None:
                with open(os.path.join(AVG_TEMP_FOLDER, country + '_monthly.csv'), 'w', encoding='utf-8-sig') as f:
                    df_avg_temp.to_csv(f)
                    self.add_multiple_dataset(df_avg_temp)
                with open(os.path.join(MAX_TEMP_FOLDER, country + '_monthly.csv'), 'w', encoding='utf-8-sig') as f:
                    df_max_temp.to_csv(f)
                    self.add_multiple_dataset(df_max_temp)
                with open(os.path.join(MIN_TEMP_FOLDER, country + '_monthly.csv'), 'w', encoding='utf-8-sig') as f:
                    df_min_temp.to_csv(f)
                    self.add_multiple_dataset(df_min_temp)
                with open(os.path.join(MM_PREC_FOLDER, country + '_monthly.csv'), 'w', encoding='utf-8-sig') as f:
                    df_mm_prec.to_csv(f)
                    self.add_multiple_dataset(df_mm_prec)

    def _make_http_request(self, body_request: Dict) -> pd.DataFrame:
        res = requests.post(CLIMATE_CHANGE_PORTAL_URL, data=body_request, headers=self._headers)
        if res.status_code == 200:
            url = res.json().get('success', None)
            if url is not None:
                url = url + "/1"
                try:
                    df = pd.read_csv(url, sep=',', header=None, names=self._csv_columns)
                    df = df.tail(df.shape[0] - 3)
                    return df
                except:
                    self.print('-Errore lettua CSV con pandas')
            else:
                self.print(f'-Errore no URL nella risposta: ({res.text})')
        else:
            self.print(f'-Errore durante richiesta POST: ({res.status_code})')
