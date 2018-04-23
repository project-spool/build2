import pandas as pd
import numpy as np
from scipy.sparse import csc_matrix


def filter_incompletes(df):
    return df.dropna()


def sample(size, df):
    return df.sample(n=size)


def convert_to_df(path):
    return pd.read_csv(path, sep='\t')


if __name__ == '__main__':

    # # declare the local file paths for the data
    # USER_DATA_PATH = '../data/usersha1-profile.tsv'
    #
    # # Read the files into pandas data frames
    #
    # # data frame to hold all of the user account data
    # user_df = convert_to_df(USER_DATA_PATH)
    #
    # # Set the column names
    # user_columns = ['user_id', 'gender', 'age', 'country', 'join_date']
    # user_df.columns = user_columns
    #
    # # filter all rows from data frame that are not complete
    # completes = filter_incompletes(user_df)
    #
    # # get a minor sample
    # sampled_data = sample(10000, completes)
    #
    # # get 10,000 random usernames
    # user_names = sampled_data.filter(items=['user_id'])
    # # artist_df.groupby('user_id').filter(lambda user_group: user_group in user_names)
    #
    #
    # # use these usernames to filter the artist plays, then concat the top 15 artists for each into a tuple
    #
    # # export new files
    # sampled_data.to_csv('../data/user_sample.tsv', sep='\t', index=False)
    # user_names.to_csv('../data/10kusernames.tsv', sep='\t', index=False)

    # TOP_ARTIST_DATA_PATH = '../data/usersha1-artmbid-artname-plays.tsv'
    # TOP_ARTIST_DATA_PATH = '../data/first_million_artists.tsv'
    # TOP_ARTIST_DATA_PATH = '../data/test-artist-ds.tsv'
    TOP_ARTIST_DATA_PATH = '../data/100klines.tsv'


    # data frame to hold all of the top artist play data
    artist_df = convert_to_df(TOP_ARTIST_DATA_PATH)
    artist_columns = ['user_id', 'artist_id', 'artist_name', 'play_count']
    artist_df.columns = artist_columns
    artist_df = filter_incompletes(artist_df)

    # split by user id
    grouped = artist_df.groupby('user_id')
    artists = artist_df.artist_id.unique()

    # used to reference back IDs and artist names
    artist_id_dict = dict(zip(artist_df.artist_id, artist_df.artist_name))

    # used for data creation when referenceing
    # sort of like a reverse array
    artist_index_dict = {}
    for idx, artist in enumerate(artists):
        artist_index_dict[artist] = idx

    print(artist_index_dict)


    # heads of vectors
    cols = ['artist_id', 'artist_name', 'play_count']

    new_data = []
    header = ['user_id']
    TOP_ARTIST_COUNT = 15

    # Artist x User matrix dimensions
    # can optimize this by only taking all of the artists that are top
    NUM_ARTISTS = len(artist_index_dict)
    NUM_USERS = len(grouped)

    # populate each of these while going through the different user and artist groups to create the sparse matrix
    # row and column coordinates for each data point
    row_indices = np.array([])
    col_indices = np.array([])

    # # the meat, contains the weights for each vector
    user_artist_data = np.array([])

    # format the headers of the
    for i in range(TOP_ARTIST_COUNT):
        header.append('top_artist_{}'.format(i+1))


    # keeps track of the current user
    current_col = 0

    # keeps track of the current artist
    current_row = 0

    #keeps track of the current listening weight
    current_data = 0

    print("Users analyzing: {}".format(len(grouped)))

    for user, data in grouped:

        # every time you get to a new user, increment a new column in the
        # print("NEW USER\n")

        # grab everything from the top15 records except the userid
        top_data = data.head(TOP_ARTIST_COUNT)
        subset = top_data[cols]

        # grab total plays to calculate the normalized playcount
        total_plays = data['play_count'].sum()

        # reassign the normalized play counts to be ratios
        subset.loc[:, 'play_count'] = subset.loc[:, 'play_count'].apply(lambda x: x/total_plays)

        # tuples = []

        for x in subset.values:

            # takes in the artist id and gets the appropriate index
            artist_index = artist_index_dict[x[0]]
            current_row = artist_index
            current_data = x[2]

            # now it's time to add the new stuff to the data arrays
            row_indices = np.append(row_indices, current_row)
            col_indices = np.append(col_indices, current_col)
            user_artist_data = np.append(user_artist_data, current_data)

            # tuples.append((tuple(x)))
        #
        # tuples.insert(0, user)
        # new_data.append(tuples)

        # increment current user column
        current_col += 1

    user_artist_mtx = csc_matrix((user_artist_data, (row_indices, col_indices)), shape=(NUM_ARTISTS, NUM_USERS))

    print(user_artist_mtx)
    print(user_artist_mtx.todense())