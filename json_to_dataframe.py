import pandas as pd
import numpy as np

#load data into CSV, then use Pandas to read them

rawData = pd.read_csv('/Users/rahulnair/Desktop/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv', delimiter='\t', encoding='utf-8')

allUsers = list()
playCounts = list()

for id in rawData['userID']:
    if id not in allUsers:
        allUsers.append(id)

rawData = rawData.drop_duplicates(subset='userID')
rawData['artistPlayCount'] = list(zip(rawData['artistName'], rawData['artistPlays']))

finalData = list(zip(rawData['userID'], rawData['artistPlayCount']))

outfile = open("topArtists.txt","w+")

for i in range(len(finalData)):
    outfile.write('\n'.join('{} {}'.format(x[0], x[1]) for x in finalData))
