import os

import pandas as pd

from base.Dataset import Dataset
from constant.AvaiableCountries import COUNTRIES
from dataset.subdataset.ClimateDataset import ClimateDataset
from dataset.subdataset.FatalitiesDataset import FatalitiesDataset
from dataset.subdataset.InflowDataset import InflowDataset
from dataset.subdataset.WelfareIndexDataset import WelfareIndexDataset
from base.Pipeline import Pipeline
from pipeline.impl.EmptyPipeline import EmptyPipeline
from step.climate.Merge2022PrecipitationStep import Merge2022PrecipitationStep
from step.inflow.Add2021PredictionStep import Add2021PredictionStep
from step.inflow.FixInflowStep import FixInflowStep
from step.inflow.MergeStep import MergeStep
from utils.DateUtils import DateUtils


def _add_monthly_inflow(final_df: pd.DataFrame, df_arrival_country: pd.DataFrame, country: str) -> pd.DataFrame:
    Inflow_country = df_arrival_country[df_arrival_country['ISO3_of_Origin'] == country]
    Inflow_country = Inflow_country.set_index("Reported_Date")
    Inflow_country.index = pd.to_datetime(Inflow_country.index)
    Inflow_country = Inflow_country.rename(columns={"Monthly_inflow": f"Monthly_inflow_{country}"})
    final_df.loc[Inflow_country.index, f"Monthly_inflow_{country}"] = Inflow_country.loc[
        Inflow_country.index, f"Monthly_inflow_{country}"]
    return final_df


class VAL2GDataset(Dataset):

    def __init__(self, pipeline: Pipeline = None, verbose: bool = False) -> None:
        if pipeline is None:
            # Default pipeline
            pipeline = EmptyPipeline()
        super().__init__("VAL2G", pipeline, verbose)

    def download(self, path: str) -> None:
        # TODO: manca la parte di aggiunta delle informazioni sul clima
        cd = ClimateDataset(verbose=True)
        cd.set_pipeline(Pipeline(name="climate_pipeline",
                                 pipeline=[
                                     Merge2022PrecipitationStep()
                                 ]))
        cd.do_pipeline(download_first=True, path=os.path.join(path, 'climate'))

        inf_ds = InflowDataset(verbose=True)
        inf_ds.set_pipeline(Pipeline(name="inflow_pipeline",
                                     pipeline=[
                                         MergeStep(enable=True, params={
                                             'climate_path': os.path.join(path, 'climate')}),
                                         FixInflowStep(enable=True),
                                         Add2021PredictionStep(enable=True)
                                     ]))
        inf_ds.do_pipeline(download_first=True, path=os.path.join(path, 'inflow'))

        fatalities_ds = FatalitiesDataset()
        fatalities_ds.do_pipeline(download_first=True, path=os.path.join(path, 'hum_events'))

        wd = WelfareIndexDataset()
        wd.do_pipeline(download_first=True, path=os.path.join(path, 'welfare_index'))

        # Build features of dataset
        index_dates = DateUtils.get_index_dates()
        attributes = []
        for country in COUNTRIES:
            monthly_inflow = "Monthly_inflow_" + str(country)
            fatalities = "fatalities_" + str(country)
            HDI = "HDI_" + str(country)
            attributes.append(monthly_inflow)
            attributes.append(fatalities)
            attributes.append(HDI)

        final_df = pd.DataFrame(columns=attributes, index=index_dates)
        final_df.index = pd.to_datetime(final_df.index)

        # Merge fatalities dataset
        df_fatalities = fatalities_ds.dataset
        for column in df_fatalities:
            final_df.loc[index_dates, f"{column}"] = df_fatalities.loc[index_dates, f"{column}"]

        # Merge welfare dataset
        df_welfare = wd.dataset
        for country in COUNTRIES:
            final_df.loc[index_dates, f"HDI_{country}"] = df_welfare.loc[index_dates, f"HDI_{country}"]

        # Merge climate dataset
        """MAX_TEMP_FOLDER = os.path.join(path, 'climate/max_temp')
        MIN_TEMP_FOLDER = os.path.join(path, 'climate/min_temp')
        AVG_TEMP_FOLDER = os.path.join(path, 'climate/avg_temp')
        MM_PREC_FOLDER = os.path.join(path, 'climate/mm_prec')
        for country in COUNTRIES:
            df_max_temp = pd.read_csv(os.path.join(MAX_TEMP_FOLDER, f'{country}_monthly.csv'))
            final_df.loc[index_dates, f"{country}"] = df_max_temp.loc[index_dates, f"{country}"]
            df_min_temp = pd.read_csv(os.path.join(MIN_TEMP_FOLDER, f'{country}_monthly.csv'))
            final_df.loc[index_dates, f"{country}"] = df_min_temp.loc[index_dates, f"{country}"]
            df_avg_temp = pd.read_csv(os.path.join(AVG_TEMP_FOLDER, f'{country}_monthly.csv'))
            final_df.loc[index_dates, f"{country}"] = df_avg_temp.loc[index_dates, f"{country}"]
            df_mm_prec = pd.read_csv(os.path.join(MM_PREC_FOLDER, f'{country}_monthly.csv'))
            final_df.loc[index_dates, f"{country}"] = df_mm_prec.loc[index_dates, f"{country}"]"""

        # Add monthly inflow
        ESP_INFLOW_FILE = os.path.join(path, 'inflow/esp_merged_cleaned.csv')
        ITA_INFLOW_FILE = os.path.join(path, 'inflow/ita_merged_cleaned.csv')
        GRC_INFLOW_FILE = os.path.join(path, 'inflow/grc_merged_cleaned.csv')
        df_esp_inflow = pd.read_csv(ESP_INFLOW_FILE)
        df_ita_inflow = pd.read_csv(ITA_INFLOW_FILE)
        df_grc_inflow = pd.read_csv(GRC_INFLOW_FILE)

        final_df_esp = final_df.copy()
        final_df_ita = final_df.copy()
        final_df_grc = final_df.copy()
        for country in COUNTRIES:
            final_df_esp = _add_monthly_inflow(final_df_esp, df_esp_inflow, country)
            final_df_ita = _add_monthly_inflow(final_df_ita, df_ita_inflow, country)
            final_df_grc = _add_monthly_inflow(final_df_grc, df_grc_inflow, country)

        # Save file
        FINAL_PATH = os.path.join(path, 'final_dataset')
        if not os.path.exists(FINAL_PATH):
            os.makedirs(FINAL_PATH)
        with open(os.path.join(FINAL_PATH, "final_esp.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_esp.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_ita.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_ita.to_csv(f)
        with open(os.path.join(FINAL_PATH, "final_grc.csv"), 'w', encoding='utf-8-sig') as f:
            final_df_grc.to_csv(f)
