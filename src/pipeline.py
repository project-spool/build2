# pipeline.py
# author: Tyler Angert
# hosts all processing and clustering processes

from process import process
from cluster import cluster
from frequent_items import find_frequent_artists
import pandas as pd


def run_clustering_pipeline(sample_size, top_artist_count, num_clusters):

    """
     Runs MiniBatch K means given a sample size and top artist count
    """

    # Grab the found user dictionary and user artist matrix from the pre processing function
    # takes a sample of 10,000 random americans and takes each of their top 5 artists
    found_users, user_artist_mtx = process(sample_size=sample_size, top_artist_count=top_artist_count)

    # Pass both into the cluster function
    cluster_groups = cluster(found_users=found_users, matrix=user_artist_mtx, num_clusters=num_clusters)

    pd.to_pickle(cluster_groups, '../data/pickles/clustered-users-{}-americans.pkl'.format(sample_size))


def run_frequent_items_pipeline(path):

    """
     Finds the frequent item sets from a given pickle
    """

    # read the data from a pickle
    clustered_groups = pd.read_pickle(path)
    frequent_artist_dict = find_frequent_artists(sample_clusters=clustered_groups)

    return frequent_artist_dict


def data2json(frequent_artists):
    print("Converting data to JSON for visualization")
    return {}


if __name__ == '__main__':

    # Run the pipeline
    # Creates a pickle
    # run_clustering_pipeline(sample_size=5000, top_artist_count=5, num_clusters=10)

    # creates the frequent items
    # Reads the previous pickle
    frequent_artists = run_frequent_items_pipeline(path='../data/pickles/clustered-users-5000-americans.pkl')

    print(frequent_artists)

    for cluster in frequent_artists.keys():
        print("\nCluster: ", cluster)
        print(frequent_artists[cluster])