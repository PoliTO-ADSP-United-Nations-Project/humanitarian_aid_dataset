import os
from typing import Any
from urllib.error import URLError

import pandas as pd

from base.Step import Step
from constant.AvaiableCountries import COUNTRIES_CURRENCY
from constant.DatasetUrl import PERCENTAGE_MONEY_CHANGE
from utils.FigshareUtils import FigshareUtils


def _add_percent_change(df_arrival_country: pd.DataFrame, percentage_money_path: str) -> pd.DataFrame:
    for country, currency in COUNTRIES_CURRENCY:
        if country == "PSE":
            df_arrival_country[f"Perc_Change_PSE"] = 0
        else:
            percent_change = pd.read_csv(os.path.join(percentage_money_path, f"{currency}_serie_storica_mensile.csv"))
            percent_change = percent_change.set_index('Data di riferimento')
            percent_change.index = pd.to_datetime(percent_change.index)
            prec_change_quot = percent_change["Quotazione"].pct_change()
            prec_change_quot = prec_change_quot.loc["2017-01-01": "2022-09-01"]
            df_arrival_country[f"Perc_Change_{country}"] = prec_change_quot
    return df_arrival_country


class AddPercentChangeStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        super().__init__("Add_Percent_Change", params, enable)

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
        if not FigshareUtils.download_datasets(PERCENTAGE_MONEY_CHANGE, os.path.join(FINAL_PATH, 'percentage_change')):
            raise URLError('Error during downloading the percentage money change dataset')

        # Get datasets
        final_df_esp = pd.read_csv(os.path.join(FINAL_PATH, 'final_esp.csv'))
        final_df_ita = pd.read_csv(os.path.join(FINAL_PATH, 'final_ita.csv'))
        final_df_grc = pd.read_csv(os.path.join(FINAL_PATH, 'final_grc.csv'))
        # Add percent change columns
        final_df_esp = _add_percent_change(final_df_esp, os.path.join(FINAL_PATH, 'percentage_change'))
        final_df_ita = _add_percent_change(final_df_ita, os.path.join(FINAL_PATH, 'percentage_change'))
        final_df_grc = _add_percent_change(final_df_grc, os.path.join(FINAL_PATH, 'percentage_change'))
        # Save file
        with open(os.path.join(FINAL_PATH, "final_esp.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_esp.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_ita.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_ita.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_grc.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_grc.to_csv(f)





