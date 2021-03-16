# -*- coding: utf-8 -*-
import logging
import json
from pathlib import Path

from pandas import read_csv


def make_series_dataset():
    logger = logging.getLogger(__name__)

    party_filepath = Path(data_dir, 'external', 'left_right_parties.json')
    with open(party_filepath) as parties_file:
        parties = json.load(parties_file)
    years = [1998, 2002, 2006, 2010, 2014, 2018]

    interim_dir = Path(data_dir, 'interim').resolve()
    parties_dir = Path(interim_dir, 'parties').resolve()

    series_dir = Path(interim_dir, 'series').resolve()
    series_dir.mkdir(exist_ok=True)
    for party in parties:
        logger.info('starting to join {} data'.format(party))
        file_path = Path(parties_dir, '1998', party, 'party.csv').resolve()
        dataset = read_csv(file_path, index_col=0)
        dataset = dataset.groupby('cod_mun').sum()

        for year in years[1:]:
            logger.info('starting to join {} data'.format(year))

            file_path = Path(parties_dir, str(
                year), party, 'party.csv').resolve()
            data = read_csv(file_path, index_col=0)
            data = data.groupby('cod_mun').sum()
            dataset = dataset.join(
                data, how='outer', rsuffix='_{}'.format(year))
            logger.info('done joining {} data'.format(year))

        dataset = dataset.rename(
            columns={"percentual_votos": "percentual_votos_" + str(years[0])})
        dataset = dataset.fillna(0)
        dataset = dataset.sort_index()

        party_dir = Path(series_dir, party).resolve()
        party_dir.mkdir(exist_ok=True)
        file_path = Path(party_dir, 'series.csv').resolve()
        dataset.to_csv(file_path)

        logger.info('done joining {} data'.format(party))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'joining parties data to make series dataset... Saving at ../data/interim/series')
    make_series_dataset()
    logger.info(
        'done joining parties data to make series dataset... Saved at ../data/interim/series')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
