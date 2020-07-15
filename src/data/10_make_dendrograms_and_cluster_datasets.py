# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, concat, Series, DataFrame

from matplotlib import pyplot as plt

from scipy.cluster.hierarchy import linkage, dendrogram, fcluster

from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score


# Code from https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
def fancy_dendrogram(*args, **kwargs):
    max_d = kwargs.pop('max_d', None)
    if max_d and 'color_threshold' not in kwargs:
        kwargs['color_threshold'] = max_d
    annotate_above = kwargs.pop('annotate_above', 0)

    ddata = dendrogram(*args, **kwargs)

    if not kwargs.get('no_plot', False):
        plt.title('Hierarchical Clustering Dendrogram (truncated)')
        plt.xlabel('sample index or (cluster size)')
        plt.ylabel('distance')
        for i, d, c in zip(ddata['icoord'], ddata['dcoord'], ddata['color_list']):
            x = 0.5 * sum(i[1:3])
            y = d[1]
            if y > annotate_above:
                plt.plot(x, y, 'o', c=c)
                plt.annotate("%.3g" % y, (x, y), xytext=(0, -5),
                             textcoords='offset points',
                             va='top', ha='center')
        if max_d:
            plt.axhline(y=max_d, c='k')
    return ddata


def make_dendrograms_and_cluster_datasets():
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

    dendrogram_dir = Path(reports_dir, 'dendrogram').resolve()
    dendrogram_dir.mkdir(exist_ok=True)

    metrics_dir = Path(processed_dir, 'metrics').resolve()
    metrics_dir.mkdir(exist_ok=True)

    metrics_metadata = {
        'silhouette': silhouette_score,
        'calinski_harabasz': calinski_harabasz_score,
        'davies_bouldin': davies_bouldin_score
    }

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

    n_clusters_values = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    for data_type, metadata in metadata_by_type.items():
        data_type_dir = Path(cluster_dir, data_type).resolve()
        data_type_dir.mkdir(exist_ok=True)

        dendrogram_data_type_dir = Path(dendrogram_dir, data_type).resolve()
        dendrogram_data_type_dir.mkdir(exist_ok=True)

        metrics_data_type_dir = Path(metrics_dir, data_type).resolve()
        metrics_data_type_dir.mkdir(exist_ok=True)

        for party in parties:
            party_dir = Path(data_type_dir, party).resolve()
            party_dir.mkdir(exist_ok=True)

            file_path = Path(metadata['dir'], party,
                             metadata['file_name']).resolve()
            dataset = read_csv(file_path)

            links = linkage(dataset.drop(columns='cod_mun'), method='ward')

            dendrogram_party_dir = Path(
                dendrogram_data_type_dir, party).resolve()
            dendrogram_party_dir.mkdir(exist_ok=True)

            dendrogram_filepath = Path(
                dendrogram_party_dir, 'dendrogram.pdf').resolve()

            plt.figure()
            fancy_dendrogram(
                links,
                truncate_mode='lastp',
                p=12,
                leaf_rotation=90.,
                leaf_font_size=12.,
                show_contracted=True,
                annotate_above=10,
            )
            plt.savefig(dendrogram_filepath)
            plt.close()

            metrics_party_dir = Path(
                metrics_data_type_dir, party).resolve()
            metrics_party_dir.mkdir(exist_ok=True)

            metrics_filepath = Path(
                metrics_party_dir, 'metrics.csv').resolve()

            metrics_frame = DataFrame()

            for n_clusters in n_clusters_values:

                logger.info(
                    f'starting to create {party} {n_clusters} cluster data with {data_type} data')

                labels = fcluster(links, n_clusters,
                                  criterion='maxclust')
                labels = Series(labels)
                labels.name = 'cluster'

                cluster_dataset = concat((dataset['cod_mun'], labels), 1)
                cluster_dataset['cod_mun'] = cluster_dataset['cod_mun'].astype(
                    int)

                n_clusters_dir = Path(party_dir, str(n_clusters)).resolve()
                n_clusters_dir.mkdir(exist_ok=True)
                file_path = Path(n_clusters_dir, 'cluster.csv')

                cluster_dataset.to_csv(file_path, index=False)

                scores = []
                for _, function in metrics_metadata.items():
                    score = function(dataset.drop(columns='cod_mun'), labels)
                    scores.append(score)

                scores = Series(scores)
                scores.name = str(n_clusters)

                metrics_frame = metrics_frame.append(scores)

                logger.info(
                    f'done creating {party} {n_clusters} cluster data')

            metrics_frame.columns = metrics_metadata.keys()
            metrics_frame.index.name = 'n_clusters'
            metrics_frame.to_csv(metrics_filepath)


def main():
    """ Runs data processing scripts to turn brazil voting data from (../interim) into
        presidential data (saved in ../interim/presidential).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'creating cluster data... Saving at ../data/processed/cluster')
    make_dendrograms_and_cluster_datasets()
    logger.info(
        'done creating cluster data... Saved at ../data/processed/cluster')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()
    reports_dir = Path(project_dir, 'reports').resolve()

    main()
