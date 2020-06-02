# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv
from geopandas import read_file

from libpysal.weights import Queen
from esda import Moran, Moran_Local

from splot.esda import plot_moran, plot_local_autocorrelation
import matplotlib.pyplot as plt

from pickle import dump


def make_moran_datasets():
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
    mesh = mesh.merge(correspondence[['CD_GEOCMU', 'COD_TSE']], how='right')
    mesh = mesh.set_index('COD_TSE')
    mesh = mesh.sort_index()

    weights = Queen.from_dataframe(mesh)

    years = [1994, 1998, 2002, 2006, 2010, 2014, 2018]

    moran_dir = Path(data_dir, 'processed', 'moran').resolve()
    moran_dir.mkdir(exist_ok=True)
    for party in parties:
        file_path = Path(series_dir, party, 'series.csv').resolve()
        dataset = read_csv(file_path)
        dataset.cod_mun = dataset.cod_mun.astype(int)
        dataset = dataset.set_index('cod_mun')
        dataset.index.name = 'COD_TSE'
        dataset = dataset.sort_index()

        party_dir = Path(moran_dir, party).resolve()
        party_dir.mkdir(exist_ok=True)
        global_dir = Path(party_dir, 'global').resolve()
        global_dir.mkdir(exist_ok=True)
        local_dir = Path(party_dir, 'local').resolve()
        local_dir.mkdir(exist_ok=True)

        dataset.columns = years
        merged_mesh = mesh.merge(dataset, on='COD_TSE')

        for year in years:
            year_dir = Path(global_dir, str(year)).resolve()
            year_dir.mkdir(exist_ok=True)
            file_path = Path(year_dir, 'global.m')

            logger.info(
                'starting to calculate global moran\'s index for {} at {}'.format(party, year))
            moran = Moran(merged_mesh[year], weights)
            with open(file_path, 'wb') as moran_file:
                dump(moran, moran_file)
            logger.info(
                'done calculating global moran\'s index for {} at {}'.format(party, year))

            year_dir = Path(local_dir, str(year)).resolve()
            year_dir.mkdir(exist_ok=True)
            file_path = Path(year_dir, 'local.m')

            logger.info(
                'starting to calculate local moran\'s index for {} at {}'.format(party, year))
            local_moran = Moran_Local(merged_mesh[year], weights)
            with open(file_path, 'wb') as local_moran_file:
                dump(local_moran, local_moran_file)
            logger.info(
                'done calculating local moran\'s index for {} at {}'.format(party, year))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'using series data to make moran dataset... Saving at ../data/processed/moran')
    make_moran_datasets()
    logger.info(
        'done making moran dataset... Saved at ../data/processed/moran')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
