import os
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from pandas.errors import EmptyDataError
from requests.adapters import HTTPAdapter
from urllib3 import Retry

usecols_dict = {
    'vilnius': [
        'Transportas',
        'Marsrutas',
        'ReisoID',
        'MasinosNumeris',
        'Ilguma',
        'Platuma',
        'Greitis',
        'Azimutas',
        'ReisoPradziaMinutemis',
        'NuokrypisSekundemis',
        'MatavimoLaikas',
        'MasinosTipas',
    ]
}

dtype_dict = {
    'vilnius': {
        'Transportas': pd.CategoricalDtype(),
        'Marsrutas': pd.StringDtype(),
        'ReisoID': pd.Int64Dtype(),
        'MasinosNumeris': pd.StringDtype(),
        'Ilguma': pd.Int64Dtype(),
        'Platuma': pd.Int64Dtype(),
        'Greitis': pd.Int64Dtype(),
        'Azimutas': pd.Int64Dtype(),
        'ReisoPradziaMinutemis': pd.Int64Dtype(),
        'NuokrypisSekundemis': pd.Int64Dtype(),
        'MatavimoLaikas': pd.Int64Dtype(),
        'MasinosTipas': pd.CategoricalDtype(),
    }
}


def fetch_vehicle_positions_df(city: str = 'vilnius') -> pd.DataFrame:
    url = f'http://www.stops.lt/{city}/gps_full.txt'
    usecols = usecols_dict[city]
    dtype = dtype_dict[city]

    with requests.Session() as s:
        retries = Retry(
            total=10,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504])

        s.mount('http://', HTTPAdapter(max_retries=retries))
        s.mount('https://', HTTPAdapter(max_retries=retries))

        r = s.get(url)

        # noinspection PyTypeChecker
        df = pd.read_csv(
            StringIO(r.text),
            on_bad_lines='warn',
            usecols=usecols,
            dtype=dtype,
        )

        df['Platuma'] = df['Platuma'] / 1000000
        df['Ilguma'] = df['Ilguma'] / 1000000

        return df


def parse_and_listen_for_vehicle_positions(started_at: datetime) -> pd.DataFrame:
    df_unique_vehicle_positions = None

    while datetime.now() - started_at < timedelta(hours=1, minutes=10):
        fetched_at = int(datetime.timestamp(datetime.now()))
        try:
            df_new_vehicle_positions = fetch_vehicle_positions_df()
        except EmptyDataError as ex:
            print(ex)
            time.sleep(3)
            continue

        df_new_vehicle_positions['Gauta'] = fetched_at

        if df_unique_vehicle_positions is None:
            df_unique_vehicle_positions = df_new_vehicle_positions

        df_unique_vehicle_positions = pd.concat([df_unique_vehicle_positions, df_new_vehicle_positions]) \
            .drop_duplicates(
            subset=[
                'Transportas',
                'Marsrutas',
                'ReisoID',
                'MasinosNumeris',
                'MasinosTipas',
                'MatavimoLaikas',
            ]
        )

        time.sleep(3)

    return df_unique_vehicle_positions


def fetch_vehicle_positions():
    started_at = datetime.now()
    df_vehicle_positions = parse_and_listen_for_vehicle_positions(started_at)

    file_date = (started_at - timedelta(hours=3, minutes=59)).date().isoformat()

    os.makedirs('data/temp', exist_ok=True)
    df_vehicle_positions.to_parquet(f'data/temp/{file_date}.parquet', index=False)


if __name__ == '__main__':
    fetch_vehicle_positions()
