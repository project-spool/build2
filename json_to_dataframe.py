import pandas as pd
import time
import numpy as np


##########################
# basic data importation
##########################

rawData = pd.read_csv("~/Desktop/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv", sep='\t')

##########################
# data cleaning
##########################

cleaned = rawData.dropna()

#########################
# grouping by the user
# APPROACH 1: classical method
#########################

users = []

startTime = time.time()
for id in cleaned['userID']:
    if users.count(id) < 5:
        users.append(id)
    else:
        pass
endTime = time.time()
print("Total time with classical method:", (endTime-startTime))
# all unique users found

#########################
# grouping by the user
# APPROACH 2: using pandas
#########################
dfN = cleaned.groupby('userID').apply(lambda x:x['artistPlays'].reset_index()).reset_index()
#print(dfN[dfN['level_1'] <= 4][['userID', 'artistPlays']])

#########################
# grouping by the user
# APPROACH 3: using pandas with an even simpler twise
#########################

time1 = time.time()
print(cleaned.groupby('userID').head(5))
time2 = time.time()
print("Total time with Lambda:",time2 - time1)