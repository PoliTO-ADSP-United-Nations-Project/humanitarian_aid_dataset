import os.path
from urllib.error import URLError

import pandas as pd

from base.Dataset import Dataset
from constant.AvaiableCountries import COUNTRIES
from constant.DatasetUrl import WELFARE_INDICIES_URL
from base.Pipeline import Pipeline
from pipeline.impl.EmptyPipeline import EmptyPipeline
from utils.DateUtils import DateUtils
from utils.FigshareUtils import FigshareUtils


class WelfareIndexDataset(Dataset):

    def __init__(self, pipeline: Pipeline = None, verbose: bool = False) -> None:
        if pipeline is None:
            # Default pipeline
            pipeline = EmptyPipeline()
        super().__init__('Welfare_Dataset', pipeline, verbose)

    def download(self, path: str) -> None:
        if not FigshareUtils.download_dataset(WELFARE_INDICIES_URL, path, 'hdr21-22.csv'):
            raise URLError('Error during downloading the Events dataset')

        # Filter by only useful countries
        df = pd.read_csv(os.path.join(path, 'hdr21-22.csv'))
        index_dates = DateUtils.get_index_dates()
        hdi_dataframe = []
        for country in COUNTRIES:
            HDI_country = df[df['iso3'] == country]
            df_hdi = pd.DataFrame(columns=[f"HDI_{country}"], index=index_dates)
            df_hdi.index = pd.to_datetime(df_hdi.index)
            for i in range(2017, 2022):
                df_hdi.loc[df_hdi.filter(regex=str(i), axis=0).index] = HDI_country[f"hdi_{i}"].values[0]
            hdi_dataframe.append(df_hdi)

        hdi = pd.concat(hdi_dataframe, axis=1)
        hdi.index = pd.to_datetime(hdi.index)
        with open(os.path.join(path, "filtered_hdr.csv"), 'w', encoding='utf-8-sig') as f:
            hdi.to_csv(f)
            self.dataset = hdi


