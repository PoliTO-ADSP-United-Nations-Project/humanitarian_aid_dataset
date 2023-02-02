import os.path
import pickle
import warnings
from typing import Any, Dict

import pandas as pd

from base.Step import Step
from constant.DatasetUrl import PROPHET_2021_CLIMATE_PREDICTION
from utils.FigshareUtils import FigshareUtils

warnings.filterwarnings('ignore')


def _map_name_into_column(name: str) -> str:
    if name == 'Min temperature':
        return 'Min_T'
    elif name == 'Max temperature':
        return 'Max_T'
    elif name == 'Mean temperature':
        return 'Avg_T'
    elif name == 'Percipitation':
        return 'mm_Percip'
    else:
        raise Exception('No mapping found ' + str(name))


def _add_2021_prediction(df: pd.DataFrame, prediction_pickle: Dict) -> pd.DataFrame:
    df = df[df.columns.drop(list(df.filter(regex='Unnamed')))]
    df['Reported_Date'] = pd.to_datetime(df['Reported_Date'], format='%Y-%m')
    df = df.query("not Monthly_inflow.isnull()", engine='python')
    df = df.query("ISO3_of_Origin != 'OOO' & ISO3_of_Origin != 'OOS'", engine='python')
    iso_list = df.loc[(df['Reported_Date'] >= '2021-01') & (df['Reported_Date'] <= '2021-12')].values
    x = 0
    for prediction in prediction_pickle:
        if x > 1:
            continue
        iso = prediction['name'][1].split('_')[0]
        column_name = _map_name_into_column(prediction['name'][0])
        if iso not in iso_list:
            continue
        ds = prediction['prediction']['ds']
        yhat = prediction['prediction']['yhat']
        if len(ds) != len(yhat):
            continue
        for i in ds:
            date = ds[i]
            y = yhat[i]
            df.loc[(df['Reported_Date'] == date) & (df['ISO3_of_Origin'] == iso), column_name] = y
    df.sort_values(by='Reported_Date', ascending=True, inplace=True)
    return df


class Add2021PredictionStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        super().__init__("Add_2021_prediction_inflow", params, enable)

    def can_execute(self, path: str = None) -> bool:
        if path is None:
            return False
        return True

    def execute(self, path: str = None) -> Any:
        if not self.can_execute(path):
            print(f'No needed to execute "{self.id}" step')
            return

        ESP_INFLOW_FILE = os.path.join(path, "esp_merged_cleaned.csv")
        ITA_INFLOW_FILE = os.path.join(path, "ita_merged_cleaned.csv")
        GRC_INFLOW_FILE = os.path.join(path, "grc_merged_cleaned.csv")
        PREDICTION_FOLDER = os.path.join(path, '2021_prophet_prediction')

        if not FigshareUtils.download_dataset(PROPHET_2021_CLIMATE_PREDICTION, PREDICTION_FOLDER,
                                              '2021_predictions_final.pkl'):
            raise ValueError('Error during download 2021 climate dataset prediction')
        elif not os.path.exists(ESP_INFLOW_FILE) or not os.path.exists(ITA_INFLOW_FILE) \
                or not os.path.exists(GRC_INFLOW_FILE):
            raise FileNotFoundError(f'Merged file not found. Please execute the step \'MergeStep\'')

        with open(os.path.join(PREDICTION_FOLDER, '2021_predictions_final.pkl'), 'rb') as f:
            predictions = pickle.load(f)
            df_esp = _add_2021_prediction(pd.read_csv(ESP_INFLOW_FILE), predictions)
            df_ita = _add_2021_prediction(pd.read_csv(ITA_INFLOW_FILE), predictions)
            df_grc = _add_2021_prediction(pd.read_csv(GRC_INFLOW_FILE), predictions)
            # Save file
            with open(os.path.join(path, 'esp_merged_cleaned.csv'), 'w', encoding='utf-8-sig') as file:
                df_esp.to_csv(file)
            with open(os.path.join(path, 'ita_merged_cleaned.csv'), 'w', encoding='utf-8-sig') as file:
                df_ita.to_csv(file)
            with open(os.path.join(path, 'grc_merged_cleaned.csv'), 'w', encoding='utf-8-sig') as file:
                df_grc.to_csv(file)
