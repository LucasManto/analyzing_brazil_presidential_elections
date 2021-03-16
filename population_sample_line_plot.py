import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

party = 'left'

dataset = pd.read_csv(f'./data/interim/series/{party}/series.csv')
clusters_2 = pd.read_csv(f'./data/processed/cluster/series/{party}/2/cluster.csv')

clusters_dataset_2 = dataset.merge(clusters_2)

# cluster_1_cities = clusters_dataset_2[clusters_dataset_2.cluster == 1].drop(columns="percentual_votos_1994")
# cluster_2_cities = clusters_dataset_2[clusters_dataset_2.cluster == 2].drop(columns="percentual_votos_1994")
cluster_1_cities = clusters_dataset_2[clusters_dataset_2.cluster == 1]
cluster_2_cities = clusters_dataset_2[clusters_dataset_2.cluster == 2]

clusters = [cluster_1_cities, cluster_2_cities]

# cmap_group1 = LinearSegmentedColormap.from_list("cmap", ["#5DA7E5", "#5DA7E6"])
# cmap_group2 = LinearSegmentedColormap.from_list("cmap", ["#097FC0", "#097FC1"])
cmap_group1 = LinearSegmentedColormap.from_list("cmap", ["#ED979E", "#ED979F"])
cmap_group2 = LinearSegmentedColormap.from_list("cmap", ["#DA5552", "#DA5553"])
cmaps = [cmap_group1, cmap_group2]

for index, cluster in enumerate(clusters):
  data = cluster.drop(columns=['cod_mun', 'cluster'])
  data = data * 100
  ax = data.sample(10).T.plot(title=None, legend=False, colormap=cmaps[index])
  ax.set_xticklabels(range(1994, 2023, 4))
  ax.set_frame_on(False)
  ax.set_ylabel('Voting shares')
  ax.set_xlabel('Election Years')
  plt.ylim(0, 100.1)

  plt.savefig(f'{party.lower()}_series_{index+1}.png')
# plt.show()