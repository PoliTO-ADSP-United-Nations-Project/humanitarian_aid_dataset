import os
from typing import Any

import pandas as pd

from base.Step import Step


class MergeStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        super().__init__("Merge", params, enable)

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
        final_df_esp = pd.read_csv(os.path.join(FINAL_PATH, 'final_esp.csv'))
        final_df_ita = pd.read_csv(os.path.join(FINAL_PATH, 'final_ita.csv'))
        final_df_grc = pd.read_csv(os.path.join(FINAL_PATH, 'final_grc.csv'))
        # Merge all datasets (ESP, ITA, GRC)
        final_df_esp = final_df_esp[lambda x: x.time_idx >= 12]
        frames = [final_df_ita, final_df_grc, final_df_esp]
        merged_df = pd.concat(frames)
        del merged_df['Unnamed: 0']
        # Save file
        with open(os.path.join(FINAL_PATH, "final_dataset.csv"), 'w', encoding='utf-8-sig') as f:
            merged_df.to_csv(f)
            os.remove(os.path.join(FINAL_PATH, 'final_esp.csv'))
            os.remove(os.path.join(FINAL_PATH, 'final_ita.csv'))
            os.remove(os.path.join(FINAL_PATH, 'final_grc.csv'))

