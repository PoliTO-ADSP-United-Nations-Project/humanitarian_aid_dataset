import os.path
from typing import Any

import pandas as pd

from base.Step import Step
from constant.DatasetUrl import NASA_URL_PRECIPITATION_2022
from utils.FigshareUtils import FigshareUtils


class Merge2022PrecipitationStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        self.LOCAL_FOLDER_NAME = "prec_2022"
        self.LOCAL_FILE_NAME = "2022_precipitation.csv"
        self.MONTH_MAPPING = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

        super().__init__("Merge_2022Precipitation", params, enable)

    def can_execute(self, path: str = None) -> bool:
        if path is None:
            return False
        PREC_FOLDER = os.path.join(path, 'mm_prec')
        if not os.path.exists(PREC_FOLDER):
            raise FileNotFoundError(f'Folder \'mm_prec\' not exists inside climate dataset; download it')
        return True

    def execute(self, path: str = None) -> Any:
        if not self.can_execute(path):
            print(f'No needed to execute "{self.id}" step')
            return
        if not FigshareUtils.download_dataset(NASA_URL_PRECIPITATION_2022, os.path.join(path, self.LOCAL_FOLDER_NAME),
                                              self.LOCAL_FILE_NAME):
            print('Error during downloading the NASA dataset')
            return

        PREC_FOLDER = os.path.join(path, 'mm_prec')

        df_2022 = pd.read_csv(os.path.join(path, self.LOCAL_FOLDER_NAME, self.LOCAL_FILE_NAME))
        new_row = {}
        last_iso = None
        last_file = None
        for index, row in df_2022.iterrows():
            if last_iso is None:
                last_iso = row['iso']

            file = os.path.join(PREC_FOLDER, row['iso'] + '_monthly.csv')
            if last_iso != row['iso'] and len(new_row) > 0:
                df_prec = pd.read_csv(last_file)
                new_row['Year'] = '2022'
                df_prec = df_prec.append(new_row, ignore_index=True)
                new_row = {}
                last_iso = row['iso']
                with open(last_file, 'w', encoding='utf-8-sig') as f:
                    df_prec.to_csv(f, index=False)

            if not os.path.exists(file):
                print(f'File {file} not exists inside {PREC_FOLDER} folder')
                continue

            new_row[self.MONTH_MAPPING[row['date']]] = row['mm_prec']
            last_file = file
