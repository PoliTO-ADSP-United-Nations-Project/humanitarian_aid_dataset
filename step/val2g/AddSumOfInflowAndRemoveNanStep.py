import os
from typing import Any

import numpy as np
import pandas as pd

from base.Step import Step


class AddSumOfInflowAndRemoveNaN(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        super().__init__("Add_SumInflow_and_RemoveNaN", params, enable)

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
        # Add sum of inflow
        final_df_esp['Sum_Inflow'] = final_df_esp[list(final_df_esp.filter(regex="Monthl"))].sum(axis=1)
        final_df_ita['Sum_Inflow'] = final_df_ita[list(final_df_ita.filter(regex="Monthl"))].sum(axis=1)
        final_df_grc['Sum_Inflow'] = final_df_grc[list(final_df_grc.filter(regex="Monthl"))].sum(axis=1)
        # Remove nan
        final_df_esp[list(final_df_esp.filter(regex="Monthl"))] = final_df_esp[
            list(final_df_esp.filter(regex="Monthl"))].replace(np.nan, 0)
        final_df_ita[list(final_df_ita.filter(regex="Monthl"))] = final_df_ita[
            list(final_df_ita.filter(regex="Monthl"))].replace(np.nan, 0)
        final_df_grc[list(final_df_grc.filter(regex="Monthl"))] = final_df_grc[
            list(final_df_grc.filter(regex="Monthl"))].replace(np.nan, 0)

        final_df_esp[list(final_df_esp.filter(regex="HDI"))] = final_df_esp[
            list(final_df_esp.filter(regex="HDI"))].fillna(method="pad")
        final_df_ita[list(final_df_ita.filter(regex="HDI"))] = final_df_ita[
            list(final_df_ita.filter(regex="HDI"))].fillna(method="pad")
        final_df_grc[list(final_df_grc.filter(regex="HDI"))] = final_df_grc[
            list(final_df_grc.filter(regex="HDI"))].fillna(method="pad")

        final_df_esp[list(final_df_esp.filter(regex="fatali"))] = final_df_esp[
            list(final_df_esp.filter(regex="fatali"))].fillna(0)
        final_df_ita[list(final_df_ita.filter(regex="fatali"))] = final_df_ita[
            list(final_df_ita.filter(regex="fatali"))].fillna(0)
        final_df_grc[list(final_df_grc.filter(regex="fatali"))] = final_df_grc[
            list(final_df_grc.filter(regex="fatali"))].fillna(0)
        # TODO: manca la pulizia dei dati climatici

        # Save file
        final_df_esp = final_df_esp.fillna(value=0)
        final_df_ita = final_df_ita.fillna(value=0)
        final_df_grc = final_df_grc.fillna(value=0)
        with open(os.path.join(FINAL_PATH, "final_esp.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_esp.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_ita.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_ita.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_grc.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_grc.to_csv(f)



