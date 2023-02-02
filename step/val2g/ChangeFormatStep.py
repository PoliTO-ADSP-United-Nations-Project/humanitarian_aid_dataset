import os
from typing import Any

import pandas as pd

from base.Step import Step
from constant.AvaiableCountries import COUNTRIES
from utils.DateUtils import DateUtils


def _change_format(df_arrival_country: pd.DataFrame, destination_country: str) -> pd.DataFrame:
    # TODO: tenere conto dei dati climatici se scegliamo di inserirli
    df_arrival_country["time_idx"] = range(0, 69)
    df_arrival_country["Destination_country"] = destination_country
    df_arrival_country = pd.DataFrame.reset_index(df_arrival_country, level=None, drop=False, inplace=False, col_level=0, col_fill='')
    dataframes = []
    for country in COUNTRIES:
        country_dataframe = df_arrival_country[list(df_arrival_country.filter(regex=country))]
        country_dataframe.rename(
            columns={f'Monthly_inflow_{country}': 'Monthly_inflow', f'fatalities_{country}': 'fatalities',
                     f"HDI_{country}": "HDI", f"Perc_Change_{country}": "Perc_Change",
                     f'distance_Km_{country}': "Distance_Departure_Destination"}, inplace=True)
        country_dataframe["Sum_Inflow"] = df_arrival_country["Sum_Inflow"]
        country_dataframe["date"] = df_arrival_country["index"]
        country_dataframe["time_idx"] = df_arrival_country["time_idx"]
        country_dataframe["Destination_country"] = df_arrival_country["Destination_country"]
        country_dataframe["Departure_country"] = country
        dataframes.append(country_dataframe)
    dataset_new = pd.concat(dataframes, ignore_index=True)
    dataset_new["month"] = dataset_new.date.dt.month.astype(str).astype("category")
    dataset_new[list(dataset_new.filter(regex="HDI"))] = dataset_new[list(dataset_new.filter(regex="HDI"))].fillna(
        method="pad")
    return dataset_new


class ChangeFormatStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        super().__init__("Change_Format", params, enable)

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

        # Get datasets
        index_dates = DateUtils.get_index_dates()
        final_df_esp = pd.read_csv(os.path.join(FINAL_PATH, 'final_esp.csv')).set_index(index_dates)
        final_df_ita = pd.read_csv(os.path.join(FINAL_PATH, 'final_ita.csv')).set_index(index_dates)
        final_df_grc = pd.read_csv(os.path.join(FINAL_PATH, 'final_grc.csv')).set_index(index_dates)
        final_df_esp.index = pd.to_datetime(final_df_esp.index)
        final_df_ita.index = pd.to_datetime(final_df_ita.index)
        final_df_grc.index = pd.to_datetime(final_df_grc.index)
        # Change format
        final_df_esp = _change_format(final_df_esp, "ESP")
        final_df_ita = _change_format(final_df_ita, "ITA")
        final_df_grc = _change_format(final_df_grc, "GRC")
        # Save file
        with open(os.path.join(FINAL_PATH, "final_esp.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_esp.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_ita.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_ita.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_grc.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_grc.to_csv(f)
