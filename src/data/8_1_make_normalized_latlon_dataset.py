# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv


def make_normalized_latlon_dataset():
    logger = logging.getLogger(__name__)

    parties = ['PT', 'PSDB']

    interim_dir = Path(data_dir, 'interim').resolve()
    processed_dir = Path(data_dir, 'processed').resolve()
    latlon_dir = Path(interim_dir, 'latlon').resolve()

    processed_latlon_dir = Path(processed_dir, 'latlon').resolve()
    processed_latlon_dir.mkdir(exist_ok=True)
    for party in parties:
        logger.info('starting to normalize {} series_latlon data'.format(party))

        file_path = Path(latlon_dir, party, 'latlon.csv').resolve()
        dataset = read_csv(file_path)

        min_latitude, max_latitude = min(
            dataset.latitude), max(dataset.latitude)
        min_longitude, max_longitude = min(
            dataset.longitude), max(dataset.longitude)

        dataset.latitude = (dataset.latitude -
                            min_latitude) / (max_latitude - min_latitude)
        dataset.longitude = (dataset.longitude -
                             min_longitude) / (max_longitude - min_longitude)

        party_dir = Path(processed_latlon_dir, party).resolve()
        party_dir.mkdir(exist_ok=True)
        file_path = Path(party_dir, 'latlon.csv').resolve()
        dataset.to_csv(file_path, index=False)

        logger.info('done normalizing {} series_latlon data'.format(party))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'normalizing series_latlon dataset... Saving at ../data/processed/latlon')
    make_normalized_latlon_dataset()
    logger.info(
        'done normalizing series_latlon dataset... Saved at ../data/processed/latlon')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
