# pipeline.py
# author: Tyler Angert
# hosts all processing and clustering processes

from process import process
from cluster import optimize, calc_sses, plot_sse, create_cluster_groups
from frequent_items import find_frequent_artists
import pandas as pd


def run_processing_pipliline(sample_size, top_artist_count):
    """
     Runs the pre processing algorithms to produce the user artist matrix
    """
    found_users, user_artist_mtx = process(sample_size=sample_size, top_artist_count=top_artist_count)
    return found_users, user_artist_mtx


# def run_clustering_pipeline(found_users, user_artist_mtx):
#
#     """
#      Runs MiniBatch K means given a sample size and top artist count
#     """
#     # Pass both into the cluster function
#     cluster_groups = cluster(found_users=found_users, matrix=user_artist_mtx, num_clusters=num_clusters)
#     pd.to_pickle(cluster_groups, '../data/pickles/clustered-users-{}-americans.pkl'.format(sample_size))
#

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

    # print("Reading user pickle")
    # found_users = pd.read_pickle('../data/pickles/20k/found-users.pkl')
    #
    # print("Reading user artist matrix pickle")
    # user_artist_mtx = pd.read_pickle('../data/pickles/20k/user-artist-mtx.pkl')

    # run_clustering_pipeline(found_users, user_artist_mtx)
    # sse = calc_sses(user_artist_mtx, 5, 20)
    # pd.to_pickle(sse, '../data/pickles/20k/sse-5-20-dict')

    # print("Reading sse pickle")
    # sse = pd.read_pickle('../data/pickles/20k/sse-5-20-dict')

    # plot_sse(sse)
    # optimal_cluster = optimize(sse)
    # clusters = create_cluster_groups(found_users, optimal_cluster['labels'])
    # pd.to_pickle(clusters, '../data/pickles/clustered-users-20000-americans.pkl')

    frequent_artists = run_frequent_items_pipeline(path='../data/pickles/clustered-users-20000-americans.pkl')

    print(frequent_artists)
    #
    # for cluster in frequent_artists.keys():
    #     print("\nCluster: ", cluster)
    #     print(frequent_artists[cluster])