import os.path
from datetime import datetime

import numpy as np
import pandas as pd

from constant.AvaiableCountries import COUNTRIES
from dataset.subdataset.HumanitarianEvenetsDataset import HumanitarianEventsDataset
from base.Pipeline import Pipeline


class FatalitiesDataset(HumanitarianEventsDataset):

    def __init__(self, pipeline: Pipeline = None, verbose: bool = False) -> None:
        super().__init__(pipeline, verbose)

    def download(self, path: str) -> None:
        super(FatalitiesDataset, self).download(path)

        # Get fatalities from events
        complete_event_dataset = pd.read_csv(os.path.join(path, 'complete_events.csv'))
        death_dataframe = []
        for country in COUNTRIES:
            country_events = complete_event_dataset[complete_event_dataset['iso3'] == country]
            country_events["Reported_Date"] = country_events["event_date"].apply(
                lambda x: (datetime.strptime(x, '%d %B %Y').strftime('%Y-%m-01')))
            prova_morti = country_events.groupby('Reported_Date').sum()
            prova_morti.index = pd.to_datetime(prova_morti.index)
            death_df = pd.DataFrame(columns=[f"fatalities_{country}"], index=prova_morti.index)
            death_df.index = pd.to_datetime(death_df.index)
            death_df[f"fatalities_{country}"] = prova_morti["fatalities"]
            death_dataframe.append(death_df)

        fatalities = pd.concat(death_dataframe, axis=1)
        fatalities = fatalities.replace(np.nan, 0)
        fatalities = fatalities.loc["2017-01-01": "2022-09-01"]
        with open(os.path.join(path, "fatalities.csv"), 'w', encoding='utf-8-sig') as f:
            fatalities.to_csv(f)
            self.dataset = fatalities
