# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 12:45:01 2023

This script converts GTFS PT service data into Line files for input into CUBE.
It aims to replace the use of the spreadsheet GTFS_to_CUBElines.xlsx
The script exports:
    - a csv file that contains all the data : tl_body_output
    - a csv file for each Service_ID
    - an optional file - Details.csv (line 169) - that contains more information on the calculations to obtain the csv files.

@author: SCHN9446
email: charlotte.schnoebelen@atkinsglobal.com

"""

import os
import sys
import pandas as pd
import numpy as np
import re
import math
import time
import datetime

# Set the time
start = time.time()

# Define the path where the scripts is run
os.path.dirname(os.path.realpath(sys.argv[0]))
print("Current working directory: {0}".format(os.getcwd()))
os.path.dirname(os.path.realpath(sys.argv[0]))

# Define function to get the time in the right format
def convert(sec):
   sec = sec % (24 * 3600)
   hour = sec // 3600
   sec %= 3600
   min = sec // 60
   sec %= 60
   #return "%d:%02d:%02d" % (hour, min, sec) 
   return "%02d:%02d" % (min, sec) 

# Import the data
trips = pd.read_csv('trips.txt')
stop_times = pd.read_csv('stop_times.txt')
StopToNode = pd.read_csv('Stop_To_Node_Lookup_2023.csv')
StopToNode.drop_duplicates()
print('Data imported.')
print('Processing data...')

# Reformat Departure and Arrival times. Give 00:00 for 24:00. The script is confused because 24:00 comes after 23:59 and it expects 00:00.
stop_times['ArrTime'] = stop_times['arrival_time'].str[:5]
stop_times['DepTime'] = stop_times['departure_time'].str[:5]

j = str(24)
replacements = {'24':'00','25':'01','26':'02','27':'03','28':'04','29':'05'}

arrival_time = []   
for i in stop_times['ArrTime']:
    if i.startswith('24'):
        i = i.replace('24','00')   
        arrival_time.append(i)
    elif i[:2]>j:
        k = int(i[:2])-int(j)
        k = str(k).zfill(2)
        i = i.replace(str(i[:2]),k)
        arrival_time.append(i)
    else:
        arrival_time.append(i)
        
departure_time = []
for i in stop_times['DepTime']:
    if i.startswith('24'):
        i = i.replace('24','00')   
        departure_time.append(i)
    elif i[:2]>j:
        k = int(i[:2])-int(j)
        k = str(k).zfill(2)
        i = i.replace(str(i[:2]),k)
        departure_time.append(i)
    else:
        departure_time.append(i)
            
stop_times['ArrTime'] = arrival_time
stop_times['DepTime'] = departure_time
     
stop_times['t_s'] = stop_times['trip_id'].apply(str) + '_' + stop_times['stop_sequence'].apply(str)

# Check trips length
#trips.shape[0]

# Create a lookup with trip_id and LineName
dictn = pd.DataFrame()
dictn['trip_id'] = trips['trip_id']
dictn['service_id'] = trips['service_id']
dictn['t_s'] = dictn['trip_id'].apply(str) + '_' + '1'
dictn['route'] = dictn.trip_id.map(trips.set_index('trip_id')['route_id'])
dictn['time'] = dictn.t_s.map(stop_times.set_index('t_s')['ArrTime'])
dictn['TIME'] = np.nan
TIME = []

for i in dictn['time']:
    i = str(i)
    i = i.replace(':','')
    TIME.append(i) 
    
dictn['TIME'] = TIME    
dictn['LINE NAME'] = dictn['route'].apply(str) + "_" + dictn['TIME'].apply(str)
dictn['Notes'] = dictn.trip_id.map(trips.set_index('trip_id')['trip_headsign'])

# Body_processing
body_processing = pd.DataFrame()
body_processing['line'] = stop_times['trip_id']
body_processing['stop #'] = stop_times['stop_sequence']
body_processing['l_s'] = stop_times['t_s']
body_processing['tp'] = body_processing.l_s.map(stop_times.set_index('t_s')['timepoint'])
body_processing['LINE NAME'] = body_processing.line.map(dictn.set_index('trip_id')['LINE NAME'])
body_processing['Stop_ID'] = body_processing.l_s.map(stop_times.set_index('t_s')['stop_id'])
body_processing['Service_id'] = body_processing.line.map(dictn.set_index('trip_id')['service_id'])
body_processing['STOP'] = '1'

# Lookup the corresponding SATURN node
vals = []
for i, j in zip(body_processing['Stop_ID'],range(0,len(body_processing))):
    a = StopToNode[StopToNode['Stop_ID'] == i]
    b = a['SATURN_Node'].tolist()
    vals.append(b)
    
vals = [x for xs in vals for x in xs]  
body_processing['N'] = vals

# Creation of new variable (indice) to calculate the column NNTIME
indice = []

for j in (range(0,len(body_processing.index))):
    if body_processing['stop #'][j] == 1:
        if body_processing['tp'][j] == 1:
            indice.append(0)
            x = body_processing['l_s'][j]
        else:
            indice.append(0)
    else:
        if body_processing['tp'][j] == 0:
            indice.append(0)
            continue
        else:
            indice.append(x)
            x=body_processing['l_s'][j]
            
body_processing["Indice"] = indice 

# Converting Departure and Arrival times
body_processing['Arrival'] = body_processing.l_s.map(stop_times.set_index('t_s')['ArrTime'])
body_processing['Depart'] = body_processing.Indice.map(stop_times.set_index('t_s')['DepTime'])
body_processing['Depart'] = (body_processing['Depart']).fillna('00:00')

# Define format of time
format = '%H:%M'

# Calculation of NNTIME
NNTIME = []
for i in range(len(body_processing)):
    if body_processing['stop #'][i] == 1:
        NNTIME.append(0)
    else:
        if body_processing['tp'][i] == 0:
            NNTIME.append(0)
        else:  
            y = datetime.datetime.strptime(body_processing['Arrival'][i],format) - datetime.datetime.strptime(body_processing['Depart'][i],format)
            y = str(y)
            y = y.replace(':', '')
            y = y.replace(',', '')
            y = y.replace(' ', '')
            y = y[:-2]
            if len(y)> 2:
                y = y[-2:]
                NNTIME.append(y)
            else:
                NNTIME.append(y)

body_processing['NNTIME'] = NNTIME


# DWELL column
#body_processing['DWELL'] = np.nan
#body_processing['Depart_dwell'] = np.nan
#body_processing['Depart_dwell'] = body_processing.l_s.map(stop_times.set_index('t_s')['DepTime'])
#DWELL = []
#
#for i in range(len(body_processing)):
#    if body_processing['NNTIME'][i] == 0:
#        DWELL.append(0)
#    else:
#        z = datetime.datetime.strptime(body_processing['Arrival'][i],format) - datetime.datetime.strptime(body_processing['Depart_dwell'][i],format)
#        z = str(z)
#        z = z.replace(':', '')
#        z = z[:-2]
#        DWELL.append(z)
#        
#body_processing['DWELL'] = DWELL

# Drop unwanted columns and duplicates rows
tl_body_output = body_processing.filter(['Service_id','LINE NAME','N','STOP','NNTIME','DWELL'], axis=1)
tl_body_output = tl_body_output.drop_duplicates()

#tl_body_output['check'] = dup        
print('Calculation finished.')

# Exporting all the data
print('Exporting all the data...')
#body_processing.to_csv('Details' + '.csv', index=False) 
tl_body_output.to_csv('tl_body-output' + '.csv', index=False)

# Filter by Service_ID
service_id_group = tl_body_output['Service_id'].unique().tolist()
print('The Service_ID are :', service_id_group)

# Find the unique values in Service_ID and export a csv file for each service_id
for i in service_id_group:  
    export = tl_body_output.loc[tl_body_output['Service_id'] == i]
    export.to_csv('Service_ID_' + str(i) + '.csv', index=False)
    print('Exporting ' + 'Service_ID' + str(i) + '.csv...')

Script_time = time.time()-start
         
print("Script finished. It took:", convert(Script_time), "seconds.")