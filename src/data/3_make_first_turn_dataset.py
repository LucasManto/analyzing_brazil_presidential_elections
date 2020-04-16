# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv


def make_first_turn_dataset():
    logger = logging.getLogger(__name__)

    years = [1994, 1998, 2002, 2006, 2010, 2014, 2018]

    correspondence_path = Path(
        data_dir, 'external', 'tse-ibge-correspondence.csv').resolve()
    correspondence = read_csv(correspondence_path)
    tse_codes = correspondence.COD_TSE

    interim_dir = Path(data_dir, 'interim').resolve()
    presidential_dir = Path(interim_dir, 'presidential').resolve()
    for year in years:
        logger.info('starting to filter {} data'.format(year))

        file_path = Path(presidential_dir,
                         str(year), 'presidential.csv').resolve()
        data = read_csv(file_path)

        # Getting votes from 1st turn
        first_turn = data[data.num_turno == 1]

        # Removing votes in transit or outside Brazil
        drop_indexes = first_turn[(first_turn.sigla_uf == 'ZZ') |
                                  (first_turn.sigla_uf == 'VT')].index
        first_turn.drop(drop_indexes, inplace=True)
        first_turn = first_turn[first_turn.cod_mun.isin(tse_codes)]

        first_turn_dir = Path(interim_dir, 'first_turn').resolve()
        first_turn_dir.mkdir(exist_ok=True)
        year_dir = Path(first_turn_dir, str(year)).resolve()
        year_dir.mkdir(exist_ok=True)

        file_path = Path(year_dir, 'first_turn.csv').resolve()
        first_turn.to_csv(file_path, index=None)
        logger.info('finished filtering {} data'.format(year))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'filtering 1st turn data... Saving at ../data/interim/1st_turn')
    make_first_turn_dataset()
    logger.info(
        'done filtering 1st turn data... Saved at ../data/interim/1st_turn')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
