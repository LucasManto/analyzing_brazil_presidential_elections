# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv


def make_congressmen_dataset():
    logger = logging.getLogger(__name__)

    years = [1998, 2002, 2006, 2010, 2014, 2018]

    interim_dir = Path(data_dir, 'interim').resolve()
    elections_1994_1998_dir = Path(
        interim_dir, 'brazil_congressmen').resolve()
    for year in years:
        logger.info('starting to filter {} data'.format(year))
        file_path = Path(elections_1994_1998_dir,
                         str(year), 'br.csv').resolve()
        data = read_csv(file_path)

        congressmen = data[data.cod_cargo == 6]

        congressmen_dir = Path(interim_dir, 'congressmen').resolve()
        congressmen_dir.mkdir(exist_ok=True)
        year_dir = Path(congressmen_dir, str(year)).resolve()
        year_dir.mkdir(exist_ok=True)

        file_path = Path(year_dir, 'congressmen.csv').resolve()
        congressmen.to_csv(file_path, index=None)
        logger.info('finished filtering {} data'.format(year))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        congressmen data (saved in ../interim/congressmen).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'filtering congressmen data... Saving at ../data/interim/congressmen')
    make_congressmen_dataset()
    logger.info(
        'done filtering congressmen data... Saved at ../data/interim/congressmen')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
