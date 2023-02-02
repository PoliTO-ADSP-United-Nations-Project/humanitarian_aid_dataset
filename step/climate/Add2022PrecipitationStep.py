import math
import os.path
from datetime import datetime, timedelta
from typing import Any, List, Tuple

import numpy as np
import pandas as pd
import pycountry

import netCDF4 as nc
from geopy import Nominatim
from geopy.exc import GeocoderTimedOut

from constant.AvaiableCountries import COUNTRIES
from base.Step import Step

import warnings

from constant.DatasetUrl import EXAMPLE_NASA_URL_PRECIPITATION_2022
from utils.FigshareUtils import FigshareUtils

warnings.filterwarnings('ignore')


# Function to find the coordinate of a given city
def _findGeocode(city: str) -> float:
    try:
        geolocator = Nominatim(user_agent="Humanitarian_Aid_Dataset")
        return geolocator.geocode(city)
    except GeocoderTimedOut:
        print('Error timeout ' + str(city))


def _retrieve_lat_lon(list_countries: List[str]) -> Tuple[List[float], List[float]]:
    latitude = []
    longitude = []
    for country in list_countries:
        if country != 'Unknown':
            loc = _findGeocode(country)
            if loc is not None:
                latitude.append(loc.latitude)
                longitude.append(loc.longitude)
            else:
                latitude.append(np.nan)
                longitude.append(np.nan)
        else:
            latitude.append(np.nan)
            longitude.append(np.nan)
    return latitude, longitude


class Add2022PrecipitationStep(Step):

    def __init__(self, params: Any = None, enable: bool = True) -> None:
        self.LOCAL_FOLDER_NAME = "prec_2022"
        self.LOCAL_FILE_NAME = "Precipitation - 21_12_2022.nc4"
        self.DELTA = 0.05

        super().__init__("Add2022PrecipitationStep", params, enable)

    def can_execute(self, path: str = None) -> bool:
        if path is None:
            return False
        if os.path.exists(os.path.join(path, self.LOCAL_FOLDER_NAME, '2022_precipitation.csv')):
            return False
        return True

    def execute(self, path: str = None) -> Any:
        if not self.can_execute(path):
            print(f'No needed to execute "{self.id}" step')
            return
        if not FigshareUtils.download_dataset(EXAMPLE_NASA_URL_PRECIPITATION_2022, os.path.join(path, self.LOCAL_FOLDER_NAME),
                                              self.LOCAL_FILE_NAME):
            print('Error during downloading the NASA dataset')
            return

        print('Starting compute 2022 mm precipitation')
        ds = nc.Dataset(os.path.join(path, self.LOCAL_FOLDER_NAME, self.LOCAL_FILE_NAME))
        final_df = pd.DataFrame(columns=['date', 'iso', 'lat', 'lon', 'mm_prec'])

        # Get variables
        precip = ds.variables['precipitationCal']
        lat_var = ds.variables['lat']
        lon_var = ds.variables['lon']
        time = ds.variables['time']
        latvals = lat_var[:]
        lonvals = lon_var[:]
        days_to_add = time[:].data[0]  # days since 1970-01-01 00:00:00Z
        starting_date = datetime.strptime('01-01-1970', '%d-%m-%Y')
        final_date = starting_date + timedelta(days=days_to_add)

        # Get LAT & LON
        countries = {}
        for country in pycountry.countries:
            countries[country.alpha_3] = country.name
        list_countries_iso = set(COUNTRIES.keys())
        list_countries = [countries.get(iso, 'Unknown') for iso in list_countries_iso]
        lat, lon = _retrieve_lat_lon(list_countries)

        # Calculate the result
        for i, country in enumerate(list_countries):
            if country == 'Unknown':
                continue
            precipitation = 'Not found'
            # IDEA: use math.isclose
            lat_indicies = np.where((latvals >= (lat[i] - self.DELTA)) & (latvals <= (lat[i] + self.DELTA)))[0]
            lon_indicies = np.where((lonvals >= (lon[i] - self.DELTA)) & (lonvals <= (lon[i] + self.DELTA)))[0]
            if len(lat_indicies) > 0 and len(lon_indicies) > 0:
                # get the most similar
                final_lat_index = lat_indicies[0]
                for lat_index in lat_indicies:
                    if math.isclose(latvals[lat_index], lat[i]):
                        final_lat_index = lat_index
                final_lon_index = lon_indicies[0]
                for lot_index in lon_indicies:
                    if math.isclose(lonvals[lot_index], lon[i]):
                        final_lon_index = lot_index
                precipitation = '%7.4f' % precip[0, final_lon_index, final_lat_index]

            '''final_df = pd.concat([final_df, pd.Series({'date': final_date.strftime('%d-%m-%Y'), 'iso': list(list_countries_iso)[i],
                                   'lat': lat[i], 'lon': lon[i], 'mm_prec': precipitation})], ignore_index=True)'''

            final_df = final_df.append(
                {'date': final_date.strftime('%d-%m-%Y'), 'iso': list(list_countries_iso)[i], 'lat': lat[i],
                 'lon': lon[i], 'mm_prec': precipitation}, ignore_index=True)

        # Save the file
        final_df = final_df[final_df['mm_prec'] != 'Not found']
        final_df["mm_prec"] = pd.to_numeric(final_df["mm_prec"], downcast="float")
        final_df['date'] = pd.to_datetime(final_df['date'])
        final_df.groupby(['iso', final_df.date.dt.month])['mm_prec'].sum()
        with open(os.path.join(path, self.LOCAL_FOLDER_NAME, '2022_precipitation.csv'), 'w', encoding='utf-8-sig') as f:
            final_df.to_csv(f)
