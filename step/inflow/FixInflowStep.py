import os.path
from typing import Any

import pandas as pd

from base.Step import Step


class FixInflowStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:

        super().__init__("Fixing_inflow", params, enable)

    def can_execute(self, path: str = None) -> bool:
        if path is None:
            return False
        ESP_INFLOW_FILE = os.path.join(path, "esp_merged.csv")
        ITA_INFLOW_FILE = os.path.join(path, "ita_merged.csv")
        GRC_INFLOW_FILE = os.path.join(path, "grc_merged.csv")
        if not os.path.exists(ESP_INFLOW_FILE) or not os.path.exists(ITA_INFLOW_FILE) \
                or not os.path.exists(GRC_INFLOW_FILE):
            raise FileNotFoundError(f'Merged file not found. Please execute the step \'MergeStep\'')
        if os.path.exists(os.path.join(path, "esp_merged_cleaned.csv")) and os.path.exists(
                os.path.join(path, "ita_merged_cleaned.csv")) and \
                os.path.exists(os.path.join(path, "grc_merged_cleaned.csv")):
            return False
        return True

    def execute(self, path: str = None) -> Any:
        if not self.can_execute(path):
            print(f'No needed to execute "{self.id}" step')
            return

        ESP_INFLOW_FILE = os.path.join(path, "esp_merged.csv")
        ITA_INFLOW_FILE = os.path.join(path, "ita_merged.csv")
        GRC_INFLOW_FILE = os.path.join(path, "grc_merged.csv")

        # ESP
        df_esp = pd.read_csv(ESP_INFLOW_FILE)
        df_esp = df_esp[df_esp.columns.drop(list(df_esp.filter(regex='Unnamed')))]
        df_esp['Reported_Date'] = pd.to_datetime(df_esp['Reported_Date'], format='%Y-%m')
        df_esp = df_esp.drop(['Distinct_check', 'Cumulative_arrivals', 'Distinct_columns_check',
                              'Cumulative_arrivals_by_country_of_arrival'], axis=1)
        # Clean dataset
        df_esp = df_esp.query("not Monthly_inflow.isnull()", engine='python')
        df_esp = df_esp.query("ISO3_of_Origin != 'OOO' & ISO3_of_Origin != 'OOS'", engine='python')
        df_esp = df_esp.query("not Min_T.isnull() & not Max_T.isnull() & not Avg_T.isnull() & not mm_Percip.isnull()",
                              engine='python')

        # ITA
        df_ita = pd.read_csv(ITA_INFLOW_FILE)
        df_ita = df_ita[df_ita.columns.drop(list(df_ita.filter(regex='Unnamed')))]
        df_ita['Reported_Date'] = pd.to_datetime(df_ita['Reported_Date'], format='%Y-%m')
        # Clean dataset
        df_ita = df_ita.query("not Monthly_inflow.isnull()", engine='python')
        df_ita = df_ita.query("ISO3_of_Origin != 'XKX'", engine='python')
        df_ita = df_ita.query("not Min_T.isnull() & not Max_T.isnull() & not Avg_T.isnull() & not mm_Percip.isnull()",
                              engine='python')

        # GRC
        df_grc = pd.read_csv(GRC_INFLOW_FILE)
        df_grc = df_grc[df_grc.columns.drop(list(df_grc.filter(regex='Unnamed')))]
        df_grc['Reported_Date'] = pd.to_datetime(df_grc['Reported_Date'], format='%Y-%m')
        df_grc = df_grc.drop(['Cumulative_arrivals_by_country_of_arrival'], axis=1)
        # Clean dataset
        df_grc = df_grc.query("not Monthly_inflow.isnull()", engine='python')
        df_grc = df_grc.query("ISO3_of_Origin != 'XKX'", engine='python')
        df_grc = df_grc.query("not Min_T.isnull() & not Max_T.isnull() & not Avg_T.isnull() & not mm_Percip.isnull()",
                              engine='python')

        # Save file
        with open(os.path.join(path, 'esp_merged_cleaned.csv'), 'w', encoding='utf-8-sig') as f:
            df_esp.to_csv(f)
            os.remove(ESP_INFLOW_FILE)
        with open(os.path.join(path, 'ita_merged_cleaned.csv'), 'w', encoding='utf-8-sig') as f:
            df_ita.to_csv(f)
            os.remove(ITA_INFLOW_FILE)
        with open(os.path.join(path, 'grc_merged_cleaned.csv'), 'w', encoding='utf-8-sig') as f:
            df_grc.to_csv(f)
            os.remove(GRC_INFLOW_FILE)
