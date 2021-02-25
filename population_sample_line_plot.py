import pandas as pd
import matplotlib.pyplot as plt

dataset = pd.read_csv('./data/interim/series/PT/series.csv')
clusters_2 = pd.read_csv('./data/processed/cluster/series/PT/2/cluster.csv')

clusters_dataset_2 = dataset.merge(clusters_2)

cluster_1_cities = clusters_dataset_2[clusters_dataset_2.cluster == 1]
cluster_2_cities = clusters_dataset_2[clusters_dataset_2.cluster == 2]

data = cluster_1_cities.drop(columns=['cod_mun', 'cluster'])
data = data * 100
ax = data.sample(10).T.plot(title='Séries de votação de municípios do grupo 1 (PT)', legend=False)
ax.set_xticklabels(range(1990, 2019, 4))
ax.set_frame_on(False)
ax.set_ylabel('Percentual')

plt.show()