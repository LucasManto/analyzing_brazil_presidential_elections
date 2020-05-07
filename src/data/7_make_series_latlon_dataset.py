# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv


def make_series_latlon_dataset():
    logger = logging.getLogger(__name__)

    parties = ['PT', 'PSDB']

    latlon_path = Path(
        data_dir, 'external', 'latlon.csv').resolve()
    latlon = read_csv(latlon_path)
    latlon = latlon.rename(columns={'COD_TSE': 'cod_mun'})
    latlon = latlon[['cod_mun', 'latitude', 'longitude']]
    latlon = latlon.set_index('cod_mun')

    interim_dir = Path(data_dir, 'interim').resolve()
    series_dir = Path(interim_dir, 'series').resolve()

    latlon_dir = Path(interim_dir, 'latlon').resolve()
    latlon_dir.mkdir(exist_ok=True)
    for party in parties:
        logger.info('starting to join latlon data')
        file_path = Path(series_dir, party, 'series.csv').resolve()
        dataset = read_csv(file_path)
        dataset.cod_mun = dataset.cod_mun.astype('int')

        dataset = dataset.join(latlon, on='cod_mun', how='inner', rsuffix='_')

        party_dir = Path(latlon_dir, party).resolve()
        party_dir.mkdir(exist_ok=True)
        file_path = Path(party_dir, 'latlon.csv').resolve()
        dataset.to_csv(file_path, index=False)

        logger.info('done joining {} data'.format(party))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'joining series data to make latlon dataset... Saving at ../data/interim/latlon')
    make_series_latlon_dataset()
    logger.info(
        'done joining series data to make series dataset... Saved at ../data/interim/latlon')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
