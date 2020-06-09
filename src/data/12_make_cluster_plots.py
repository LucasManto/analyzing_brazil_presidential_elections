# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, concat, Series

from geopandas import read_file

from matplotlib import pyplot as plt


def make_cluster_dataset():
    logger = logging.getLogger(__name__)

    parties = ['PT', 'PSDB']
    n_clusters_values = [3, 5, 10]

    external_dir = Path(data_dir, 'external').resolve()

    correspondence_path = Path(
        external_dir, 'tse-ibge-correspondence.csv').resolve()
    correspondence = read_csv(correspondence_path)

    mesh_path = Path(external_dir, 'cities.json').resolve()
    mesh = read_file(mesh_path)
    mesh.CD_GEOCMU = mesh.CD_GEOCMU.astype(int)
    mesh = mesh.merge(correspondence[['CD_GEOCMU', 'COD_TSE']], how='right')
    mesh = mesh.set_index('COD_TSE')
    mesh = mesh.sort_index()

    processed_dir = Path(data_dir, 'processed').resolve()
    cluster_dir = Path(processed_dir, 'cluster').resolve()
    series_dir = Path(cluster_dir, 'series').resolve()
    lat_lon_dir = Path(cluster_dir, 'normalized_lat_lon').resolve()
    epsg_4326_dir = Path(cluster_dir, 'epsg_4326').resolve()
    epsg_4674_dir = Path(cluster_dir, 'epsg_4674').resolve()

    plots_cluster_dir = Path(reports_dir, 'cluster').resolve()
    plots_cluster_dir.mkdir(exist_ok=True)
    plots_series_dir = Path(plots_cluster_dir, 'series').resolve()
    plots_series_dir.mkdir(exist_ok=True)
    plots_lat_lon_dir = Path(plots_cluster_dir, 'lat_lon').resolve()
    plots_lat_lon_dir.mkdir(exist_ok=True)
    plots_epsg_4326_dir = Path(plots_cluster_dir, 'epsg_4326').resolve()
    plots_epsg_4326_dir.mkdir(exist_ok=True)
    plots_epsg_4674_dir = Path(plots_cluster_dir, 'epsg_4674').resolve()
    plots_epsg_4674_dir.mkdir(exist_ok=True)

    metadata_by_type = {
        'series': {
            'data_dir': series_dir,
            'plot_dir': plots_series_dir,
        },
        'normalized_lat_lon': {
            'data_dir': lat_lon_dir,
            'plot_dir': plots_lat_lon_dir,
        },
        'epsg_4326': {
            'data_dir': epsg_4326_dir,
            'plot_dir': plots_epsg_4326_dir,
        },
        'epsg_4674': {
            'data_dir': epsg_4674_dir,
            'plot_dir': plots_epsg_4674_dir,
        }
    }

    for data_type, metadata in metadata_by_type.items():
        data_type_dir = metadata['data_dir']
        plot_data_type_dir = metadata['plot_dir']

        for party in parties:
            party_dir = Path(data_type_dir, party).resolve()
            plot_party_dir = Path(plot_data_type_dir, party).resolve()
            plot_party_dir.mkdir(exist_ok=True)

            for n_clusters in n_clusters_values:
                logger.info(
                    f'starting to create {party} {n_clusters} clusters data with {data_type} data')

                file_path = Path(party_dir, str(n_clusters),
                                 'cluster.csv').resolve()
                dataset = read_csv(file_path)
                dataset = dataset.rename(columns={'cod_mun': 'COD_TSE'})

                merged_mesh = mesh.merge(dataset, on='COD_TSE')
                merged_mesh.plot(column='cluster', cmap='tab10', legend=True, categorical=True, legend_kwds={
                                 'loc': 'lower right'})
                plt.axis(False)

                plot_n_clusters_dir = Path(
                    plot_party_dir, str(n_clusters)).resolve()
                plot_n_clusters_dir.mkdir(exist_ok=True)
                file_path = Path(plot_n_clusters_dir, 'map.pdf')
                plt.suptitle(f'{party}, {data_type}, {n_clusters} clusters')
                plt.savefig(file_path)
                plt.close()

                logger.info(
                    f'done creating {party} {n_clusters} cluster data')


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'creating cluster data... Saving at ../data/reports/cluster')
    make_cluster_dataset()
    logger.info(
        'done creating cluster data... Saved at ../data/reports/cluster')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()
    reports_dir = Path(project_dir, 'reports').resolve()

    main()
