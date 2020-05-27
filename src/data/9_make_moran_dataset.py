# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv
from geopandas import read_file

from libpysal.weights import Rook, Queen
from esda import Moran, Moran_Local


def make_moran_dataset():
    logger = logging.getLogger(__name__)

    parties = ['PT', 'PSDB']

    interim_dir = Path(data_dir, 'interim').resolve()
    external_dir = Path(data_dir, 'external').resolve()
    series_dir = Path(interim_dir, 'series').resolve()

    mesh_path = Path(external_dir, 'cities.json')
    correspondence_path = Path(external_dir, 'tse-ibge-correspondence.csv')
    mesh = read_file(mesh_path)
    correspondence = read_csv(correspondence_path)

    mesh.CD_GEOCMU = mesh.CD_GEOCMU.astype(int)
    mesh = mesh.merge(correspondence.CD_GEOCMU, how='right')
    weights = Queen.from_dataframe(mesh, 'geometry', idVariable='CD_GEOCMU')

    moran_dir = Path(data_dir, 'processed', 'moran').resolve()
    moran_dir.mkdir(exist_ok=True)
    for party in parties:
        file_path = Path(series_dir, party, 'series.csv').resolve()
        dataset = read_csv(file_path)

        logger.info(
            'starting to calculate global moran\'s index for {}'.format(party))
        global_moran = dataset.drop(columns='cod_mun').apply(
            lambda column: Moran(column, weights).I)
        print(global_moran)
        logger.info(
            'done calculating global moran\'s index for {}'.format(party))

        logger.info(
            'starting to calculate local moran\'s index for {}'.format(party))
        local_moran = dataset.apply(
            lambda column: Moran_Local(column.values, weights).Is if column.name != 'cod_mun' else column)
        logger.info(
            'done calculating local moran\'s index for {}'.format(party))

        party_dir = Path(moran_dir, party).resolve()
        party_dir.mkdir(exist_ok=True)

        global_dir = Path(party_dir, 'global').resolve()
        global_dir.mkdir(exist_ok=True)
        local_dir = Path(party_dir, 'local').resolve()
        local_dir.mkdir(exist_ok=True)

        global_file_path = Path(global_dir, 'moran.csv').resolve()
        global_moran.to_csv(global_file_path, header=False)
        local_file_path = Path(local_dir, 'moran.csv').resolve()
        local_moran.to_csv(local_file_path, index=False)


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'using series data to make moran dataset... Saving at ../data/processed/moran')
    make_moran_dataset()
    logger.info(
        'done making moran dataset... Saved at ../data/processed/moran')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
