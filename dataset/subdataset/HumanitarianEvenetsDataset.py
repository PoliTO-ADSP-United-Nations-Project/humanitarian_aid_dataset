import os
from urllib.error import URLError

import pandas as pd

from base.Dataset import Dataset
from constant.DatasetUrl import HUMAN_EVENTS_URL
from base.Pipeline import Pipeline
from pipeline.impl.EmptyPipeline import EmptyPipeline
from utils.FigshareUtils import FigshareUtils


class HumanitarianEventsDataset(Dataset):

    def __init__(self, pipeline: Pipeline = None, verbose: bool = False) -> None:
        if pipeline is None:
            # Default pipeline
            pipeline = EmptyPipeline()
        super().__init__("Events_Dataset", pipeline=pipeline, verbose=verbose)

    def download(self, path: str) -> None:
        # DataSets Events 1
        if not FigshareUtils.download_datasets(HUMAN_EVENTS_URL, path):
            raise URLError('Error during downloading the Events dataset')

        # Create the complete datasets
        if os.path.exists(os.path.join(path, "complete_events.csv")):
            print('Dataset "complete_events.csv" already exists')
            return

        complete_event_dataset = pd.DataFrame()
        for file in os.listdir(path):
            if '.csv' in file:
                df = pd.read_csv(os.path.join(path, file), on_bad_lines='skip', sep=";")
                complete_event_dataset = pd.concat([complete_event_dataset, df])
                os.remove(os.path.join(path, file))

        with open(os.path.join(path, "complete_events.csv"), 'w', encoding='utf-8-sig') as f:
            complete_event_dataset.to_csv(f)
            self.dataset = complete_event_dataset
