import os.path
from typing import Any

import pandas as pd
from tqdm import tqdm

from base.Step import Step
from utils.FileUtils import FileUtils


def _put_climate_data(climate_path: str, iso: str, type_of_climate: str, name_of_column: str,
                      df: pd.DataFrame) -> pd.DataFrame:
    file = FileUtils.find_files(str(iso) + '*.csv', climate_path + "/" + str(type_of_climate))
    if len(file) > 0:
        file = file[0]
    else:
        print(
            f'-File doesn\'t exists: {climate_path + "/" + str(type_of_climate) + "/" + str(iso) + "*.csv"}')
        return df

    df_climate = pd.read_csv(file)
    df_climate = df_climate[df_climate.columns.drop(list(df_climate.filter(regex='Unnamed')))]

    for index, row in df_climate.iterrows():
        date_base = str(int(row["Year"])) + "-"
        for month in range(1, 13):
            date = date_base + "0" + str(month) if month < 9 else date_base + str(month)
            query = "`Reported_Date` == @date and `ISO3_of_Origin` == @iso"
            df.loc[df.eval(query), name_of_column] = row.iloc[month]
    return df


def _merge_inflow_dataset(path: str, climate_path: str) -> pd.DataFrame:
    final_df = pd.DataFrame()
    for file_inflow in tqdm(os.listdir(path), desc="Merging inflow dataset from"):
        df = pd.read_excel(os.path.join(path, file_inflow))
        df.columns = df.columns.str.replace(' ', '_')
        df = df[df.columns.drop(list(df.filter(regex='Unnamed')))]
        # Set type of data
        df['Reported_Date'] = df['Reported_Date'].dt.strftime('%Y-%m')
        df = df.sort_values(by=['ISO3_of_Origin', 'Reported_Date'])

        # Add climate information
        for iso in df['ISO3_of_Origin'].unique():
            df = _put_climate_data(climate_path, iso, 'min_temp', 'Min_T', df)
            df = _put_climate_data(climate_path, iso, 'max_temp', 'Max_T', df)
            df = _put_climate_data(climate_path, iso, 'avg_temp', 'Avg_T', df)
            df = _put_climate_data(climate_path, iso, 'mm_prec', 'mm_Percip', df)
        # Merge dfs into only one df
        final_df = pd.concat([final_df, df])
        print(f'dataset length: {len(final_df)}')
    return final_df


class MergeStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        if params is None or not isinstance(params, dict):
            raise ValueError(f'To use this step you need to pass a parameter like this:\n{{"climate_path": "climate '
                             f'dataset path"}}')
        if 'climate_path' not in params:
            raise ValueError('Missing key "climate_path" inside params')

        super().__init__("Inflow_Merge_Step", params, enable)

    def can_execute(self, path: str = None) -> bool:
        if path is None:
            return False
        ESP_FOLDER = os.path.join(path, "esp")
        ITA_FOLDER = os.path.join(path, "ita")
        GRC_FOLDER = os.path.join(path, "grc")
        if not os.path.exists(ESP_FOLDER) or not os.path.exists(ITA_FOLDER) \
                or not os.path.exists(GRC_FOLDER):
            raise FileNotFoundError('Error: no inflow datasets found (check inside ESP, GRC or ITA)')
        if os.path.exists(os.path.join(path, "esp_merged.csv")) and os.path.exists(
                os.path.join(path, "ita_merged.csv")) and \
                os.path.exists(os.path.join(path, "grc_merged.csv")):
            return False
        return True

    def execute(self, path: str = None) -> Any:
        if not self.can_execute(path):
            print(f'No needed to execute "{self.id}" step')
            return

        ESP_FOLDER = os.path.join(path, "esp")
        ITA_FOLDER = os.path.join(path, "ita")
        GRC_FOLDER = os.path.join(path, "grc")

        # Merge inflow dataset
        final_esp_df = _merge_inflow_dataset(ESP_FOLDER, self.params['climate_path'])
        final_ita_df = _merge_inflow_dataset(ITA_FOLDER, self.params['climate_path'])
        final_grc_df = _merge_inflow_dataset(GRC_FOLDER, self.params['climate_path'])

        # Save dataset
        with open(os.path.join(path, "esp_merged.csv"), 'w', encoding='utf-8-sig') as f:
            final_esp_df.to_csv(f)
        with open(os.path.join(path, "ita_merged.csv"), 'w', encoding='utf-8-sig') as f:
            final_ita_df.to_csv(f)
        with open(os.path.join(path, "grc_merged.csv"), 'w', encoding='utf-8-sig') as f:
            final_grc_df.to_csv(f)
