# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv


def make_parties_dataset():
    logger = logging.getLogger(__name__)

    years = [1994, 1998, 2002, 2006, 2010, 2014, 2018]
    parties = ['PT', 'PSDB']

    interim_dir = Path(data_dir, 'interim').resolve()
    percentual_dir = Path(interim_dir, 'percentual').resolve()

    parties_dir = Path(interim_dir, 'parties').resolve()
    parties_dir.mkdir(exist_ok=True)
    for year in years:
        logger.info('starting to filter {} data'.format(year))

        year_dir = Path(parties_dir, str(year)).resolve()
        year_dir.mkdir(exist_ok=True)

        file_path = Path(percentual_dir, str(year), 'percentual.csv').resolve()
        data = read_csv(file_path)
        for party in parties:
            logger.info(
                'starting to filter {} data from {}'.format(year, party))

            party_dir = Path(year_dir, party).resolve()
            party_dir.mkdir(exist_ok=True)

            file_path = Path(party_dir, 'party.csv').resolve()
            party_data = data[data.sigla_partido == party]
            party_data = party_data[['cod_mun', 'percentual_votos']]
            party_data.to_csv(file_path, index=None)

            logger.info(
                'finished filtering {} data from {}'.format(year, party))

        logger.info('finished filtering {} data'.format(year))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'filtering percentual votes to make parties dataset... Saving at ../data/interim/parties')
    make_parties_dataset()
    logger.info(
        'done filtering percentual votes to make parties dataset... Saved at ../data/interim/parties')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
