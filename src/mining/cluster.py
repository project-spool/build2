# cluster.py
# author: Tyler Angert
# takes a user x artist matrix and performs k-means clustering

import pandas as pd
from sklearn.cluster import MiniBatchKMeans, KMeans


def cluster(found_users, matrix, num_clusters):

    """
     Clusters a given matrix with MiniBatchKMeans
     Returns the grouped clusters for frequent itemset mining
    """
    print("Clustering with Mini Batch K Means")

    # grab the clustering object and fit it to the input USER x ARTIST matrix
    # km = KMeans(n_clusters=num_clusters)
    km = MiniBatchKMeans(n_clusters=num_clusters)
    km = km.fit(matrix)

    cluster_labels = km.labels_.tolist()

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