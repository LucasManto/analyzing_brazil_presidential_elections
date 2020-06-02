# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, concat, Series

from sklearn.cluster import AgglomerativeClustering


def make_cluster_dataset():
    logger = logging.getLogger(__name__)

    parties = ['PT', 'PSDB']

    interim_dir = Path(data_dir, 'interim').resolve()
    series_dir = Path(interim_dir, 'series').resolve()
    processed_dir = Path(data_dir, 'processed').resolve()
    latlon_dir = Path(processed_dir, 'latlon').resolve()
    epsg_4326_dir = Path(processed_dir, 'epsg_4326').resolve()
    epsg_4674_dir = Path(processed_dir, 'epsg_4674').resolve()
    cluster_dir = Path(processed_dir, 'cluster').resolve()
    cluster_dir.mkdir(exist_ok=True)

    metadata_by_type = {
        'series': {
            'dir': series_dir,
            'file_name': 'series.csv'
        },
        'normalized_lat_lon': {
            'dir': latlon_dir,
            'file_name': 'latlon.csv'
        },
        'epsg_4326': {
            'dir': epsg_4326_dir,
            'file_name': 'latlon.csv'
        },
        'epsg_4674': {
            'dir': epsg_4674_dir,
            'file_name': 'latlon.csv'
        }
    }

    n_clusters_values = [3, 5, 27]
    for data_type, metadata in metadata_by_type.items():
        data_type_dir = Path(cluster_dir, data_type).resolve()
        data_type_dir.mkdir(exist_ok=True)

        for party in parties:
            party_dir = Path(data_type_dir, party).resolve()
            party_dir.mkdir(exist_ok=True)

            for n_clusters in n_clusters_values:
                model = AgglomerativeClustering(
                    n_clusters=n_clusters, affinity='euclidean', linkage='ward')

                logger.info(
                    f'starting to create {party} {n_clusters} cluster data with {data_type} data')

                file_path = Path(metadata['dir'], party,
                                 metadata['file_name']).resolve()
                dataset = read_csv(file_path)

                labels = model.fit_predict(
                    dataset.drop(columns=['cod_mun']).values)
                labels = Series(labels)
                labels.name = 'cluster'

                cluster_dataset = concat((dataset['cod_mun'], labels), 1)
                cluster_dataset['cod_mun'] = cluster_dataset['cod_mun'].astype(
                    int)

                n_clusters_dir = Path(party_dir, str(n_clusters)).resolve()
                n_clusters_dir.mkdir(exist_ok=True)
                file_path = Path(n_clusters_dir, 'cluster.csv')

                cluster_dataset.to_csv(file_path, index=False)

                logger.info(
                    f'done creating {party} {n_clusters} cluster data')


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'creating cluster data... Saving at ../data/processed/cluster')
    make_cluster_dataset()
    logger.info(
        'done creating cluster data... Saved at ../data/processed/cluster')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
