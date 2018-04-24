# cluster.py
# author: Tyler Angert
# takes a user x artist matrix and performs k-means clustering

import pandas as pd
from sklearn.cluster import MiniBatchKMeans, KMeans
from sklearn import metrics
from scipy.spatial.distance import cdist
import numpy as np
import matplotlib.pyplot as plt


def calc_sses(matrix, start, stop):
    """
     Calculates the SSEs for an input matrix between start and stop k values
    """

    # for each key k:
        # k: (silhouette score, labels)
    sse = {}

    for k in range(start, stop+1):

        print("\nRunning on k = {}".format(k))

        km = MiniBatchKMeans(n_clusters=k)
        km = km.fit(matrix)

        print("Labels: {}".format(km.labels_))
        print("Inertia: {}".format(km.inertia_))

        sse[k] = (km.inertia_, km.labels_.tolist())

    return sse


def optimize(sse):

    """
     Takes in a dictionary of all SSEs for given k's and returns a tuple of the min SSE and their cluster labels
    """
    kv_pairs = zip(sse.keys(), sse.values())
    min_result = min(kv_pairs, key=lambda pair: pair[1])

    result_dict = {
        "k": min_result[0],
        "sse": min_result[1][0],
        "labels": min_result[1][1]
    }

    return result_dict


def plot_sse(sse):

    plt.figure()
    plt.interactive(False)
    plt.plot(list(sse.keys()), [val[0] for val in sse.values()])
    plt.xlabel("Number of cluster")
    plt.ylabel("SSE")
    plt.title("K-Means SSE for 20,000 Last.fm users")
    plt.show()


def create_cluster_groups(found_users, cluster_labels):

    # stores all of the user-cluster label data to be passed into a new pandas data frame
    all_tuples = []

    # iterates through the user-cluster tuple combinations, then references the found users to get top artists
    for user, clust in zip(found_users.keys(), cluster_labels):
        top_artists = found_users[user]
        tup = (clust, user, top_artists)
        all_tuples.append(tup)

    # creates a data frame from all the tuples
    user_cluster_df = pd.DataFrame(all_tuples)
    user_cluster_df.columns = ['cluster', 'user_id', 'top_artists']

    print("cluster value counts: ")
    print(user_cluster_df.cluster.value_counts())

    # groups all users by cluster for easy analysis
    cluster_groups = user_cluster_df.groupby('cluster')

    for name, group in cluster_groups:
        print(group)
        print(name)
        print(group)
        print('\n')

    # pass the grouped clusters to the frequent itemset mining
    return cluster_groups