# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, read_excel, concat, Series

from geopandas import read_file

from matplotlib import pyplot as plt


def make_cluster_dataset():
    logger = logging.getLogger(__name__)

    n_clusters_values = [2, 3]

    external_dir = Path(data_dir, 'external').resolve()

    mesh_path = Path(external_dir, 'cities.json').resolve()
    mesh = read_file(mesh_path)
    mesh.CD_GEOCMU = mesh.CD_GEOCMU.astype(int)

    processed_dir = Path(data_dir, 'processed').resolve()
    cluster_dir = Path(processed_dir, 'cluster_hdi').resolve()

    plots_cluster_dir = Path(reports_dir, 'cluster_hdi').resolve()
    plots_cluster_dir.mkdir(exist_ok=True)

    for n_clusters in n_clusters_values:
        logger.info(
            f'starting to create {n_clusters} clusters data')

        file_path = Path(cluster_dir, str(n_clusters),
                         'cluster.csv').resolve()
        dataset = read_csv(file_path)
        dataset = dataset.rename(columns={'Código': 'COD_GEOCMU'})
        dataset.CD_GEOCMU = dataset.CD_GEOCMU.astype(int)

        merged_mesh = mesh.merge(dataset, on='CD_GEOCMU')
        merged_mesh.plot(column='cluster', cmap='tab20c', legend=True, categorical=True, legend_kwds={
            'loc': 'lower right'})
        plt.axis(False)

        plot_n_clusters_dir = Path(
            plots_cluster_dir, str(n_clusters)).resolve()
        plot_n_clusters_dir.mkdir(exist_ok=True)
        file_path = Path(plot_n_clusters_dir, 'map.pdf')
        plt.suptitle(f'{n_clusters} grupos')
        plt.savefig(file_path)
        plt.close()

        logger.info(
            f'done creating {n_clusters} cluster data')


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'creating cluster data... Saving at ../data/reports/cluster_hdi')
    make_cluster_dataset()
    logger.info(
        'done creating cluster data... Saved at ../data/reports/cluster_hdi')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()
    reports_dir = Path(project_dir, 'reports').resolve()

    main()
