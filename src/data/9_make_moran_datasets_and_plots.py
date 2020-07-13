# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, DataFrame
from geopandas import read_file

from libpysal.weights import Queen
from esda import Moran, Moran_Local

from splot.esda import plot_moran, plot_local_autocorrelation
import matplotlib.pyplot as plt


def make_moran_datasets_and_plots():
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

    logger.info('starting to calculate weights...')
    weights = Queen.from_dataframe(mesh)
    logger.info(
        f'done calculating weights')

    years = [1994, 1998, 2002, 2006, 2010, 2014, 2018]

    moran_dir = Path(data_dir, 'processed', 'moran').resolve()
    moran_dir.mkdir(exist_ok=True)

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

        plots_party_dir = Path(moran_plots_dir, party).resolve()
        plots_party_dir.mkdir(exist_ok=True)
        plots_global_dir = Path(plots_party_dir, 'global').resolve()
        plots_global_dir.mkdir(exist_ok=True)
        plots_local_dir = Path(plots_party_dir, 'local').resolve()
        plots_local_dir.mkdir(exist_ok=True)

        dataset.columns = years
        merged_mesh = mesh.merge(dataset, on='COD_TSE')

        moran_values = []
        p_values = []

        for year in years:

            logger.info(
                'starting to calculate global moran\'s index for {} at {}'.format(party, year))

            moran = Moran(merged_mesh[year], weights)
            moran_values.append(moran.I)
            p_values.append(moran.p_sim)

            fig, ax = plot_moran(moran, zstandard=True, figsize=(10, 4))
            ax[0].set_title('Distribuição referência')
            ax[1].set_title(
                f'Gráfico de disperção de Moran ({round(moran.I, 2)})')
            ax[1].set_ylabel(None)
            fig.suptitle(f'{party}, {year}')

            plots_year_dir = Path(plots_global_dir, str(year)).resolve()
            plots_year_dir.mkdir(exist_ok=True)
            file_path = Path(plots_year_dir, 'global.pdf').resolve()
            fig.savefig(file_path)
            plt.close(fig)

            logger.info(
                'done calculating global moran\'s index for {} at {}'.format(party, year))

            logger.info(
                'starting to calculate local moran\'s index for {} at {}'.format(party, year))

            local_moran = Moran_Local(merged_mesh[year], weights)

            fig, ax = plot_local_autocorrelation(
                local_moran, merged_mesh, year)
            ax[0].set_title('Gráfico de disperção Moran Local')
            ax[0].set_ylabel(None)
            fig.suptitle(f'{party}, {year}')

            plots_year_dir = Path(plots_local_dir, str(year)).resolve()
            plots_year_dir.mkdir(exist_ok=True)
            file_path = Path(plots_year_dir, 'local.pdf').resolve()
            fig.savefig(file_path)
            plt.close(fig)

            logger.info(
                'done calculating local moran\'s index for {} at {}'.format(party, year))

        p_value_path = Path(party_dir, 'p_values.csv')
        p_value_frame = DataFrame({
            'moran': moran_values,
            'p_value': p_values
        }, years)
        p_value_frame.to_csv(p_value_path, index=False)

        fig = plt.figure()
        ax = plt.subplot()
        ax.plot(p_value_frame.moran)
        ax.set_xticklabels([0, 1994, 1998, 2002, 2006, 2010, 2014, 2018])
        ax.set_title(f'Índice de Moran e p-valores ({party})')
        ax.set_frame_on(False)
        ax.tick_params(axis='both', length=0, labelsize=12)
        for i in range(p_value_frame.shape[0]):
            ax.annotate(f'{round(p_value_frame.iloc[i, 0], 3)} ({p_value_frame.iloc[i, 1]})', xy=(
                i, p_value_frame.iloc[i, 0]))
        # fig.add_axes(ax)
        fig_path = Path(plots_party_dir, 'moran_values.pdf').resolve()
        plt.savefig(fig_path)
        plt.close(fig)


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'using series data to make moran dataset... Saving at ../reports/moran')
    make_moran_datasets_and_plots()
    logger.info(
        'done making moran dataset... Saved at ../reports/moran')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()
    reports_dir = Path(project_dir, 'reports').resolve()

    main()
