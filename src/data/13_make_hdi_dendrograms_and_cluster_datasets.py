# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, read_excel, concat, Series, DataFrame

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

    processed_dir = Path(data_dir, 'processed').resolve()
    cluster_dir = Path(processed_dir, 'cluster_hdi').resolve()
    cluster_dir.mkdir(exist_ok=True)

    dendrogram_dir = Path(reports_dir, 'dendrogram_hdi').resolve()
    dendrogram_dir.mkdir(exist_ok=True)

    metrics_dir = Path(processed_dir, 'metrics_hdi').resolve()
    metrics_dir.mkdir(exist_ok=True)

    metrics_metadata = {
        'silhouette': silhouette_score,
        'calinski_harabasz': calinski_harabasz_score,
        'davies_bouldin': davies_bouldin_score
    }

    n_clusters_values = [2, 3]

    file_path = Path(data_dir, 'raw', 'hdi.xlsx').resolve()
    dataset = read_excel(file_path, encoding='latin', sep=';')
    dataset = dataset.rename(columns={'Código': 'CD_GEOCMU'})
    dataset.CD_GEOCMU = dataset.CD_GEOCMU.astype(int)

    links = linkage(dataset.drop(
        columns=['CD_GEOCMU', 'Espacialidades']), method='ward')

    dendrogram_filepath = Path(
        dendrogram_dir, 'dendrogram.pdf').resolve()

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

    metrics_filepath = Path(
        metrics_dir, 'metrics.csv').resolve()

    metrics_frame = DataFrame()

    for n_clusters in n_clusters_values:

        logger.info(
            f'starting to create {n_clusters} cluster data')

        labels = fcluster(links, n_clusters,
                          criterion='maxclust')
        labels = Series(labels)
        labels.name = 'cluster'

        cluster_dataset = concat((dataset['CD_GEOCMU'], labels), 1)

        n_clusters_dir = Path(cluster_dir, str(n_clusters)).resolve()
        n_clusters_dir.mkdir(exist_ok=True)
        file_path = Path(n_clusters_dir, 'cluster.csv')

        cluster_dataset.to_csv(file_path, index=False)

        scores = []
        for _, function in metrics_metadata.items():
            score = function(dataset.drop(
                columns=['CD_GEOCMU', 'Espacialidades']), labels)
            scores.append(score)

        scores = Series(scores)
        scores.name = str(n_clusters)

        metrics_frame = metrics_frame.append(scores)

        logger.info(
            f'done creating {n_clusters} cluster data')

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
