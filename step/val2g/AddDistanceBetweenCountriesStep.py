import os
from typing import Any
from urllib.error import URLError

import pandas as pd

from base.Step import Step
from constant.AvaiableCountries import COUNTRIES
from constant.DatasetUrl import DISTANCE_BETWEEN_COUNTRIES
from utils.FigshareUtils import FigshareUtils


class AddDistanceBetweenCountriesStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        super().__init__("Add_Distance_Between_Countries", params, enable)

    def can_execute(self, path: str = None) -> bool:
        if path is None:
            return False
        if not os.path.exists(os.path.join(path, "final_esp.csv")) or not os.path.exists(
                os.path.join(path, "final_ita.csv")) or \
                not os.path.exists(os.path.join(path, "final_grc.csv")):
            raise FileNotFoundError('Missing mandatory files (the output of VAL2GDataset download)')
        return True

    def execute(self, path: str = None) -> Any:
        FINAL_PATH = os.path.join(path, 'final_dataset')
        if not self.can_execute(FINAL_PATH):
            print(f'No needed to execute "{self.id}" step')
            return

        # Download
        if not FigshareUtils.download_dataset(DISTANCE_BETWEEN_COUNTRIES,
                                              os.path.join(FINAL_PATH, 'distance_countries'),
                                              'countries_pairwise_distances.csv'):
            raise URLError('Error during downloading the distance between african countries dataset')

        # Get datasets
        final_df_esp = pd.read_csv(os.path.join(FINAL_PATH, 'final_esp.csv'))
        final_df_ita = pd.read_csv(os.path.join(FINAL_PATH, 'final_ita.csv'))
        final_df_grc = pd.read_csv(os.path.join(FINAL_PATH, 'final_grc.csv'))
        distance_countries = pd.read_csv(os.path.join(FINAL_PATH, 'distance_countries', 'countries_pairwise_distances'
                                                                                        '.csv'))
        distance_countries = distance_countries.set_index('Unnamed: 0')
        # Get the distance
        for country in COUNTRIES:
            final_df_esp[f'distance_Km_{country}'] = distance_countries.loc['ESP'][f"{country}"] / 1000
            final_df_ita[f'distance_Km_{country}'] = distance_countries.loc['ITA'][f"{country}"] / 1000
            final_df_grc[f'distance_Km_{country}'] = distance_countries.loc['GRC'][f"{country}"] / 1000
        # Save file
        with open(os.path.join(FINAL_PATH, "final_esp.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_esp.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_ita.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_ita.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_grc.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_grc.to_csv(f)
