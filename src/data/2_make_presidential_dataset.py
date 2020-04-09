# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv


def make_presidential_dataset():
    years = [1994, 1998, 2002, 2006, 2010, 2014, 2018]

    interim_dir = Path(data_dir, 'interim').resolve()
    elections_1994_1998_dir = Path(
        interim_dir, 'brazil').resolve()
    for year in years:
        file_path = Path(elections_1994_1998_dir,
                         str(year), 'br.csv').resolve()
        data = read_csv(file_path)

        presidential = data[data.cod_cargo == 1]

        presidential_dir = Path(interim_dir, 'presidential').resolve()
        presidential_dir.mkdir(exist_ok=True)
        year_dir = Path(presidential_dir, str(year)).resolve()
        year_dir.mkdir(exist_ok=True)

        file_path = Path(year_dir, 'presidential.csv').resolve()
        presidential.to_csv(file_path, index=None)


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'filtering presidential data... Saving at ../data/interim/presidential')
    make_presidential_dataset()
    logger.info(
        'done filtering presidential data... Saved at ../data/interim/presidential')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
