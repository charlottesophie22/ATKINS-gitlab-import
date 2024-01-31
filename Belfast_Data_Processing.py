# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 15:53:45 2022

@author: Charlotte Schnoebelen
Transport and Data Analytics Consultant
ATKINS

Belfast journeys data processing

"""
import os
import sys
import pandas as pd
import numpy as np
import re

# Define the path where the scripts is run
os.path.dirname(os.path.realpath(sys.argv[0]))

# Import the data
df1 = pd.read_csv('Atkins Metro Journeys wc 071019.csv')
df2 = pd.read_csv('Atkins Ulsterbus Journeys wc 071019.csv', encoding='unicode_escape')
df3 = pd.read_csv('Atkins Metro Ulsterbus wc 071019.csv', encoding='unicode_escape')
df4 = pd.read_csv('Atkins Glider Journeys wc 071019.csv')

# Check the length
df1.shape[0]
df2.shape[0]
df3.shape[0]
df4.shape[0]
df3.count()
df4.count()

# Rename columns
df3 = df3.rename(columns = {'ProductCount':'JourneyCount'})
df4 = df4.rename(columns = {'BRTJourneyCount':'JourneyCount'})

# Keep only the relevant columns
df1 = df1[['JourneyStartDate','LocationDescription', 'RouteNumber','DirectionOfTravel', 'BoardingStageName','BoardingHour','JourneyCount']]
df2 = df2[['JourneyStartDate','LocationDescription', 'RouteNumber','DirectionOfTravel', 'BoardingStageName','BoardingHour','JourneyCount']]
df3 = df3[['JourneyStartDate','LocationDescription', 'RouteNumber','DirectionOfTravel', 'BoardingStageName','BoardingHour','JourneyCount']]
df4 = df4[['JourneyStartDate','LocationDescription','BoardingStageName','BoardingHour','JourneyCount']]

# Merge the data
concat = pd.concat([df1,df2,df3,df4], ignore_index=True, axis=0)
print(concat)

# Create extra column to filter the Services
concat['Type'] = concat['LocationDescription'].str.split(')').str[0]
concat['Type'] = concat['Type'].str.split('(').str[1]
print(concat['Type'].unique())
concat.fillna('', inplace=True)

# Export the total data
#concat.to_csv('Merged_journeys.csv', index=False) !!! too big to read on Excel !!!

# Split the data by Services
Metro = concat.loc[concat['Type'] == 'Metro']
UB = concat.loc[concat['Type'] == 'UB']
MU = concat.loc[concat['Type'] == 'MU']
nan = concat.loc[concat['Type'] == '']

# Export the dataframes by Services
Metro.to_csv('1.Metro.csv', index=False)
UB.to_csv('2.UB.csv', index=False)
MU.to_csv('3.MU.csv', index=False)
nan.to_csv('4.NaN.csv', index=False)

print('Script finished.')