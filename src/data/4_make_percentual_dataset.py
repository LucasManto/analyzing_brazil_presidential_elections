# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv


def make_percentual_dataset():
    logger = logging.getLogger(__name__)

    years = [1994, 1998, 2002, 2006, 2010, 2014, 2018]

    interim_dir = Path(data_dir, 'interim').resolve()
    first_turn_dir = Path(interim_dir, 'first_turn').resolve()
    for year in years:
        logger.info('starting to transform {} data'.format(year))
        file_path = Path(first_turn_dir,
                         str(year), 'first_turn.csv').resolve()
        data = read_csv(file_path)

        percentual = data.copy()
        percentual['percentual_votos'] = 0
        for city in percentual.nome_mun.unique():
            city_rows = percentual[percentual.nome_mun == city].index
            city_nominal_votes = percentual.loc[city_rows,
                                                'qtde_votos_nominais']

            percentual.loc[city_rows, 'percentual_votes'] = city_nominal_votes / \
                city_nominal_votes.sum()

        percentual_dir = Path(interim_dir, 'percentual').resolve()
        percentual_dir.mkdir(exist_ok=True)
        year_dir = Path(percentual_dir, str(year)).resolve()
        year_dir.mkdir(exist_ok=True)

        file_path = Path(year_dir, 'percentual.csv').resolve()
        percentual.to_csv(file_path, index=None)
        logger.info('finished transforming {} data'.format(year))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'transforming absolute votes in percentual votes... Saving at ../data/interim/percentual')
    make_percentual_dataset()
    logger.info(
        'transforming absolute votes in percentual votes... Saved at ../data/interim/percentual')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
