# pipeline.py
# author: Tyler Angert
# hosts all processing and clustering processes

from process import process
from cluster import optimize, calc_sses, plot_sse, create_cluster_groups, get_all_results
from frequent_items import find_frequent_artists
import pandas as pd
import json


def run_processing_pipeline(sample_size, top_artist_count):
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
    #
    # for k in frequent_artists:
    #     print(k)
    #     print(frequent_artists[k])
    #     print('\n')

    # Initial setup
    viz_dict = {}
    viz_dict["name"] = '20k Last.fm Users'
    viz_dict["children"] = []

    # Go through each of the children
    for k in frequent_artists:

        current_cluster = {}
        artists = frequent_artists[k]

        children = []

        for artist in artists:
            artist_obj = {}
            artist_obj["name"] = artist[0]
            artist_obj["size"] = artist[1]
            children.append(artist_obj)

        # gets the name of the top artist
        if(len(artists) == 0):
            name = ""
        else:
            name = artists[0][0]

        current_cluster["name"] = name
        current_cluster["children"] = children

        viz_dict["children"].append(current_cluster)

    return viz_dict


def export_json(dict, filename):
    with open(filename, 'w') as f:
        json.dump(dict, f, ensure_ascii=False, indent=4, separators=(',', ': '))
        f.write('\n')

if __name__ == '__main__':

    # Run the pipeline
    SIZE = 20000

    # FIXME: Export
    # found_users, user_artist_mtx = run_processing_pipeline(SIZE, 15)
    # pd.to_pickle(found_users, '../data/pickles/{}/found-users.pkl'.format(SIZE))
    # pd.to_pickle(user_artist_mtx, '../data/pickles/{}/user-artist-mtx.pkl'.format(SIZE))

    # print("Reading user pickle")
    # found_users = pd.read_pickle('../data/pickles/{}/found-users.pkl'.format(SIZE))
    #
    # print("Reading user artist matrix pickle")
    # user_artist_mtx = pd.read_pickle('../data/pickles/{}/user-artist-mtx.pkl'.format(SIZE))
    #
    # # run_clustering_pipeline(found_users, user_artist_mtx)
    START = 5
    STOP = 30
    # sse = calc_sses(user_artist_mtx, START, STOP)
    # pd.to_pickle(sse, '../data/pickles/{}/sse-{}-{}-dict.pkl'.format(SIZE, START, STOP))
    #
    # print("Reading sse pickle")

    sse = pd.read_pickle('../data/pickles/{}/sse-{}-{}-dict.pkl'.format(SIZE, START, STOP))

    print(sse)


    # plot_sse(sse)
    # optimal_cluster = optimize(sse)
    # clusters = create_cluster_groups(found_users, optimal_cluster['labels'])

    results = get_all_results(sse)

    for result in results:

        frequent_artists = run_frequent_items_pipeline(path='../data/pickles/{}/clustered-users-{}-americans.pkl'.format(SIZE, result['k']))
        viz = data2json(frequent_artists)
        export_json(viz, 'clustered-users-{}-{}-americans.json'.format(result['k'], SIZE))