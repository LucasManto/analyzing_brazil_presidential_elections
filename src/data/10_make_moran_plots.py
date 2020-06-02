# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv
from geopandas import read_file

from libpysal.weights import Queen
from esda import Moran, Moran_Local

from splot.esda import plot_moran, plot_local_autocorrelation
import matplotlib.pyplot as plt

from pickle import load


def make_moran_plots():
    logger = logging.getLogger(__name__)

    parties = ['PT', 'PSDB']

    processed_dir = Path(data_dir, 'processed').resolve()
    external_dir = Path(data_dir, 'external').resolve()
    series_dir = Path(data_dir, 'interim', 'series').resolve()
    moran_dir = Path(processed_dir, 'moran').resolve()

    mesh_path = Path(external_dir, 'cities.json')
    correspondence_path = Path(external_dir, 'tse-ibge-correspondence.csv')
    mesh = read_file(mesh_path)
    correspondence = read_csv(correspondence_path)

    mesh.CD_GEOCMU = mesh.CD_GEOCMU.astype(int)
    mesh = mesh.merge(correspondence[['CD_GEOCMU', 'COD_TSE']], how='right')
    mesh = mesh.set_index('COD_TSE')
    mesh = mesh.sort_index()

    years = [1994, 1998, 2002, 2006, 2010, 2014, 2018]

    moran_plots_dir = Path(reports_dir, 'moran').resolve()
    moran_plots_dir.mkdir(exist_ok=True)
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

        plots_party_dir = Path(moran_plots_dir, party).resolve()
        plots_party_dir.mkdir(exist_ok=True)
        plots_global_dir = Path(plots_party_dir, 'global').resolve()
        plots_global_dir.mkdir(exist_ok=True)
        plots_local_dir = Path(plots_party_dir, 'local').resolve()
        plots_local_dir.mkdir(exist_ok=True)

        dataset.columns = years
        merged_mesh = mesh.merge(dataset, on='COD_TSE')

        for year in years:
            year_dir = Path(global_dir, str(year)).resolve()
            logger.info(
                'starting to make global moran\'s plot for {} at {}'.format(party, year))

            file_path = Path(year_dir, 'global.m').resolve()
            moran = None
            with open(file_path, 'rb') as moran_file:
                moran = load(moran_file)

            fig, _ = plot_moran(moran, zstandard=True, figsize=(10, 4))
            fig.suptitle(f'{party}, {year}')

            plots_year_dir = Path(plots_global_dir, str(year)).resolve()
            plots_year_dir.mkdir(exist_ok=True)
            file_path = Path(plots_year_dir, 'global.pdf').resolve()
            fig.savefig(file_path)
            plt.close(fig)

            logger.info(
                'done making global moran\'s plot for {} at {}'.format(party, year))

            year_dir = Path(local_dir, str(year)).resolve()
            logger.info(
                'starting to make local moran\'s plot for {} at {}'.format(party, year))

            file_path = Path(year_dir, 'local.m').resolve()
            local_moran = None
            with open(file_path, 'rb') as local_moran_file:
                local_moran = load(local_moran_file)

            fig, _ = plot_local_autocorrelation(local_moran, merged_mesh, year)
            fig.suptitle(f'{party}, {year}')

            plots_year_dir = Path(plots_local_dir, str(year)).resolve()
            plots_year_dir.mkdir(exist_ok=True)
            file_path = Path(plots_year_dir, 'local.pdf').resolve()
            fig.savefig(file_path)
            plt.close(fig)

            logger.info(
                'done making local moran\'s plot for {} at {}'.format(party, year))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../processed) into
        presidential data (saved in ../processed/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'using moran data to make moran plots... Saving at ../reports/moran')
    make_moran_plots()
    logger.info(
        'done making moran plots... Saved at ../reports/moran')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()
    reports_dir = Path(project_dir, 'reports').resolve()

    main()
