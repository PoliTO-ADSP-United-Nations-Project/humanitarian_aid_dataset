import os.path

from base.Dataset import Dataset
from constant.DatasetUrl import INFLOW_ESP_2017_2022_URL, INFLOW_ITA_2017_2022_URL, INFLOW_GRC_2017_2022_URL
from base.Pipeline import Pipeline
from pipeline.impl.EmptyPipeline import EmptyPipeline
from utils.FigshareUtils import FigshareUtils


class InflowDataset(Dataset):

    def __init__(self, pipeline: Pipeline = None, verbose: bool = False) -> None:
        if pipeline is None:
            # Default pipeline
            pipeline = EmptyPipeline()
        super().__init__("Inflow_Dataset", pipeline=pipeline, verbose=verbose)

    def download(self, path: str) -> None:
        # Download ESP Inflow dataset
        if not FigshareUtils.download_datasets(INFLOW_ESP_2017_2022_URL, os.path.join(path, "esp")):
            print('Error during downloading the ESP Inflow dataset')
            return
        # Download ITA Inflow dataset
        if not FigshareUtils.download_datasets(INFLOW_ITA_2017_2022_URL, os.path.join(path, "ita")):
            print('Error during downloading the ITA Inflow dataset')
            return
        # Download GRC Inflow dataset
        if not FigshareUtils.download_datasets(INFLOW_GRC_2017_2022_URL, os.path.join(path, "grc")):
            print('Error during downloading the GRC Inflow dataset')
            return

