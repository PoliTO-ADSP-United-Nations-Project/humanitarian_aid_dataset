import os
from datetime import datetime
from typing import Any
from urllib.error import URLError

import pandas as pd

from base.Step import Step
from constant.AvaiableCountries import COUNTRIES
from constant.DatasetUrl import INFLOW_2022_ESP_GRC_ITA
from utils.DateUtils import DateUtils
from utils.FigshareUtils import FigshareUtils


def _add_2022_inflow(df_arrival_country: pd.DataFrame, file_path_2022: str) -> pd.DataFrame:
    Inflow_2022 = pd.read_excel(file_path_2022)
    Inflow_2022 = Inflow_2022.dropna(thresh=1, axis=0)
    Inflow_2022["Reported_Date"] = Inflow_2022["Reported_Date"].apply(
        lambda x: (datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-01')))
    for country in COUNTRIES:
        Inflow_country = Inflow_2022[Inflow_2022['ISO3_of_Origin'] == country]
        Inflow_country = Inflow_country.set_index("Reported_Date")
        Inflow_country.index = pd.to_datetime(Inflow_country.index)
        Inflow_country = Inflow_country.rename(columns={"Monthly_inflow": f"Monthly_inflow_{country}"})
        df_arrival_country.loc[Inflow_country.index, f"Monthly_inflow_{country}"] = Inflow_country.loc[
            Inflow_country.index, f"Monthly_inflow_{country}"]
    return df_arrival_country


class Add2022MonthlyInflow(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        super().__init__("Add_2022_Monthly_Inflow", params, enable)

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
        if not FigshareUtils.download_datasets(INFLOW_2022_ESP_GRC_ITA, os.path.join(FINAL_PATH, '2022_inflow')):
            raise URLError('Error during downloading the 2022 monthly inflows dataset')

        # Get datasets
        index_dates = DateUtils.get_index_dates()
        final_df_esp = pd.read_csv(os.path.join(FINAL_PATH, 'final_esp.csv')).set_index(index_dates)
        final_df_ita = pd.read_csv(os.path.join(FINAL_PATH, 'final_ita.csv')).set_index(index_dates)
        final_df_grc = pd.read_csv(os.path.join(FINAL_PATH, 'final_grc.csv')).set_index(index_dates)
        final_df_esp.index = pd.to_datetime(final_df_esp.index)
        final_df_ita.index = pd.to_datetime(final_df_ita.index)
        final_df_grc.index = pd.to_datetime(final_df_grc.index)
        # Add monthly inflow for ESP, ITA, GRC
        final_df_esp = _add_2022_inflow(final_df_esp, os.path.join(FINAL_PATH, '2022_inflow/2022 Arrivals_to_Europe - '
                                                                               'ESP.xlsx'))
        final_df_ita = _add_2022_inflow(final_df_ita, os.path.join(FINAL_PATH, '2022_inflow/2022 Arrivals_to_Europe - '
                                                                               'ITA.xlsx'))
        final_df_grc = _add_2022_inflow(final_df_grc, os.path.join(FINAL_PATH, '2022_inflow/2022 Arrivals_to_Europe - '
                                                                               'GRC.xlsx'))

        with open(os.path.join(FINAL_PATH, "final_esp.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_esp.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_ita.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_ita.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_grc.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_grc.to_csv(f)
