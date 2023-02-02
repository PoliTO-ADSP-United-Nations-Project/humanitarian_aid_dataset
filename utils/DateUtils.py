from datetime import datetime, timedelta

import numpy as np
import pandas as pd


class DateUtils:

    @staticmethod
    def get_index_dates() -> pd.DatetimeIndex:
        Time_range_day = np.arange(datetime(2017, 1, 1), datetime(2022, 10, 1), timedelta(days=1)).astype(datetime)
        Time_range_month = np.array(Time_range_day, dtype='datetime64[M]')
        set_ = set(Time_range_month)
        Time_range_month = list(set_)
        return pd.DatetimeIndex(np.array(Time_range_month, dtype='datetime64[M]'))
