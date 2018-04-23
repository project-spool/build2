import pandas as pd
import numpy as np
import pickle
import random
from scipy.sparse import csr_matrix, csc_matrix
from sklearn.cluster import AgglomerativeClustering, KMeans, MeanShift


# MARK: Pandas pre-processing functions
def filter_incompletes(df):
    return df.dropna()


def sample(size, df):
    return df.sample(n=size)


def convert_to_df(path):
    return pd.read_csv(path, sep='\t')


def grab_random_data(usernames):
    return 0


def create_random_user_tsv(num, users):

    # user_ids = user_artist_df['user_id'].unique()
    # create_random_user_tsv(10000, user_ids)

    random_users = random.sample(list(users), num)
    random_users_df = pd.DataFrame(random_users, columns=['user_id'])
    random_users_df.to_csv('../data/random_users.tsv', sep='\t', index=False)


# calculates the tf-idf score for each artist
def tf_idf(term):

    # TF(t) = (Number of times term t appears in a document) / (Total number of terms in the document).
    tf = 5

    # IDF(t) = log_e(Total number of documents / Number of documents with term t in it).
    # create a dictionary that maps a term to how many users have it
    idf = 5

    return tf * idf


if __name__ == '__main__':

    # grab all of the users from the US
    # ALL_USER_PROFILES = '../data/raw/usersha1-profile.tsv'
    # # all_user_profs_df = convert_to_df(ALL_USER_PROFILES)
    # all_user_profs_df = pd.read_csv(ALL_USER_PROFILES, sep='\t')
    # all_user_profs_df = filter_incompletes(all_user_profs_df)
    #
    # print(all_user_profs_df)
    # all_user_profs_df.columns = ['user_id', 'gender', 'age', 'country', 'join_data']
    # all_user_profs_df = all_user_profs_df[all_user_profs_df.country == 'United States']
    # all_user_profs_df.to_csv('../data/cleaned/united-states-users.tsv',sep='\t')
    #
    # AMERICAN_USERS = '../data/cleaned/united-states-users.tsv'
    # american_users_df = convert_to_df(AMERICAN_USERS)
    # american_users_df.to_pickle('../data/pickles/american-users-pickle.pkl')

    american_users_df = pd.read_pickle('../data/pickles/american-users-pickle.pkl')
    random_americans = sample(1500, american_users_df)

    # Previously used to create the data frames
    # RANDOM_USERS = '../data/random-users/1000-random-users.tsv'
    # TOP_ARTIST_DATA_PATH = '../data/all-cleaned.tsv'

    # data frame to hold all of the top artist play data
    # user_artist_df = convert_to_df(TOP_ARTIST_DATA_PATH)
    # user_artist_cols = ['user_id', 'artist_id', 'artist_name', 'play_count']
    # user_artist_df.columns = user_artist_cols
    # user_artist_df = filter_incompletes(user_artist_df)
    #
    # user_artist_df.to_pickle('../data/pickles/user-artist-df-pickle.pkl')
    #
    # # data frame to hold all of the random user ids
    # user_id_df = convert_to_df(RANDOM_USERS)
    # user_id_df.to_pickle('../data/pickles/random-users-pickle.pkl')

    # Read the pickled data frames
    # user_id_df = pd.read_pickle('../data/pickles/random-users-pickle.pkl')
    TOP_ARTIST_COUNT = 5

    # user_artist_df = pd.read_pickle('../data/pickles/user-artist-df-pickle.pkl')
    # user_id_groups = user_artist_df.groupby('user_id')
    # pd.to_pickle(user_id_groups, '../data/pickles/user-id-groups.pkl')

    user_ids = random_americans.user_id.unique()
    user_id_groups = pd.read_pickle('../data/pickles/user-id-groups.pkl')

    # used to reference back IDs and artist names
    # FIRST PASS: grab the artists from the relevant artists
    artist_id_dict = {}

    print("GETTING RELEVANT ARTISTS")
    for uid in user_ids:

        try:
            group = user_id_groups.get_group(uid)
        except KeyError as err:
            print("whoops")

        top_data = group.head(TOP_ARTIST_COUNT)
        artist_ids = list(top_data['artist_id'])
        artist_names = list(top_data['artist_name'])
        zipped = zip(artist_ids, artist_names)

        for k, v in zipped:
            artist_id_dict[k] = v

    # FIXME: create another dictionary for the users
    # this will be used to assign the cluster labels to each user
    # used for data creation when referenceing
    # sort of like a reverse array

    user_index_dict = {}
    artist_index_dict = {}

    for idx, artist in enumerate(artist_id_dict.keys()):
        artist_index_dict[artist] = idx

    # column names for the pandas artist data frame
    cols = ['artist_id', 'artist_name', 'play_count']
    # populate each of these while going through the different user and artist groups to create the sparse matrix
    # row and column coordinates for each data point
    row_indices = np.array([])
    col_indices = np.array([])

    # # the meat, contains the weights for each vector
    user_artist_data = np.array([])

    # keeps track of the current user
    current_row = 0

    # keeps track of the current artist
    current_col = 0

    # keeps track of the current listening weight
    current_data = 0

    print("Users analyzing: {}".format(len(user_ids)))

    # stores tuples for preprocessed array
    # will convert into another pandas data frame then export to tsv
    col_headers = ['user_id']
    for i in range(TOP_ARTIST_COUNT):
        col_headers.append('top_artist_{}'.format(i+1))

    formatted_tuples = []
    err_count = 0

    # grab each of the random samples
    found_users = {}


    for uid in user_ids:

        # grab the group
        try:

            group = user_id_groups.get_group(uid)
            found_users[uid] = []

        except KeyError as err:
            err_count += 1
            print("couldn't find {} users".format(err_count))
            continue

        top_data = group.head(TOP_ARTIST_COUNT)
        subset = top_data[cols]

        # FIXME: include tf-idf to discount large globally popular artists
        # grab total plays to calculate the normalized playcount
        total_plays = top_data['play_count'].sum()

        # stores all of the tuples
        tuples = list()

        # # first add the user id
        # tuples.append(uid)
        # tuples.append(current_row)

        for x in subset.values:

            # takes in the artist id and gets the appropriate index
            artist_id = x[0]
            artist_index = artist_index_dict[x[0]]
            play_count = x[2]

            current_col = artist_index

            # this is the current "term frequency"
            current_data = (play_count / total_plays)

            # now it's time to add the new stuff to the data arrays
            row_indices = np.append(row_indices, current_row)
            col_indices = np.append(col_indices, current_col)
            user_artist_data = np.append(user_artist_data, 1/current_data)

            tup = (artist_id, artist_id_dict[artist_id])
            tuples.append(tup)

            # print("play count: {}, total plays: {}, ratio: {}".format(play_count, total_plays, current_data))
            # print("col: {} | row: {} | data: {}".format(current_col, current_row, current_data))

        found_users[uid] = tuples

        # increment current user row
        current_row += 1

    # create the row-centric matrix
    user_artist_mtx = csr_matrix((user_artist_data, (row_indices, col_indices)))
    dense_mtx = user_artist_mtx.toarray()
    print(dense_mtx)

    # Finally, cluster the data
    km = KMeans(n_clusters=10)
    km = km.fit(dense_mtx)
    clusters = km.labels_.tolist()

    print("clusters length: ", len(clusters))
    print("found users length: ", len(found_users))
    print(clusters)

    all_tuples = []

    for user, cluster in zip(found_users.keys(), clusters):
        top_artists = found_users[user]
        tup = (cluster, user, top_artists)
        all_tuples.append(tup)

    user_cluster_df = pd.DataFrame(all_tuples)
    user_cluster_df.columns = ['cluster', 'user_id', 'top_artists']

    cluster_groups = user_cluster_df.groupby('cluster')

    for name, group in cluster_groups:
        print(name)
        print(group)
        print('\n')


