import pandas as pd
from pymining import itemmining, assocrules, perftesting
import math


if __name__ == '__main__':

    # sample cluster data on 5000 random american users, k = 10 for k means, and top 5 artists
    sample_clusters = pd.read_pickle('../data/pickles/sample-clustered-data.pkl')

    for cluster, group in sample_clusters:

        print('\n')
        print("CLUSTER NUMBER {}".format(cluster))

        num_users = len(group.user_id)
        min_sup = math.floor(num_users/15)

        if num_users > 1000:
            min_sup = 75

        print("min sup: ", min_sup)
        print("number of users: {}".format(num_users))

        transactions = (list(group.top_artists))
        relim_input = itemmining.get_relim_input(transactions)
        report = itemmining.relim(relim_input, min_support=min_sup)
        report = [(list(item)[0][1], report[item]) for item in report if len(item) == 1]
        report = sorted(report, key=lambda tup: tup[1], reverse=True)
        print(report)