# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 11:53:29 2023
Python version - 3.8.8

@author: Charlotte Schnoebelen
Transport planner
ATKINS

Path building process - version 3.0

The script picks up the shortest paths between start/end nodes. 

This version include several improvements:
    
    (1) the shortest path selected between two nodes is based on the distance between the 
        nodes instead of the number of intermediate nodes in the chain.* 
    (2) Once the shortest path is selected, it is stored in a dictionnary so that it can be
        picked up if met again later on.
    (3) The nodes that do not exist are flagged and stop the script.

*Note that the number of paths can be edited in the "to edit" section below. 

For further information please consult the script document "PT_Lines_Script_Guidelines.docx"

"""
import os
import sys
from sys import exit
import pandas as pd
import numpy as np
import re
import itertools
from itertools import chain
import time
import re

###~~~~~~~~~~~~~~~TO EDIT~~~~~~~~~~~~~~~~~~~###

# Define the number of paths to consider
Nb_path = 3

# Read the data
df = pd.read_csv('body_processing.csv')
lib = pd.read_csv('User_Links_N6.csv') 

###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~###

# Set the time
start = time.time()

# Define the path where the scripts is run
cwd = os.getcwd()
print("Current working directory: {0}".format(os.getcwd()))
os.path.dirname(os.path.realpath(sys.argv[0]))

# Replace problematic nodes
df['N'] = df['N'].replace(51620,51619)
df['N'] = df['N'].replace(51615,51614)
df['N'] = df['N'].replace(51704,51510)
df['N'] = df['N'].replace(51703,51510)  

# Drop duplicate rows
df.drop_duplicates()

# Declare dictionnary
dictn = {}
dictn_d = {}
dictn_f = {}

# Store nodes in list
ANodes = lib['ANode'].to_list()
BNodes = lib['BNode'].to_list()

# Check that nodes exist, if not stop the script

print('Checking that all the Nodes exist...')
investigate = []

for i in df['N']:
    if not i in ANodes:
        investigate.append(i)
if not len(investigate)==0:
    investigate = [*set(investigate)]
    print('The following nodes are unknown:', investigate)
    exit()                                                      
else:
    print('All the nodes exist.')
    
# Define new output file
df1= df.iloc[[0],:]
    
# Define function to get the time in the right format
def convert(sec):
   sec = sec % (24 * 3600)
   hour = sec // 3600
   sec %= 3600
   min = sec // 60
   sec %= 60
   return "%d:%02d:%02d" % (hour, min, sec) 
   #return "%02d:%02d" % (min, sec) 

# Get info on the number of line names and nodes to connect
print('The line names are :',df['LINE NAME'].unique())
print('There are ',(len(df.index)-1),'nodes to connect')
    
# Processing the data to get the shortest paths between nodes
for i, j in zip(df['N'],(range(1,len(df.index)))):
    start_node = time.time()
    print('Processing Row',j,'( Node',i,')')
    if not (df['LINE NAME'][j]) == (df['LINE NAME'][j-1]):
            nextrow = df.iloc[j]
            df1 = df1.append(nextrow, ignore_index=True)
            continue
    else:  
        stop = df['N'].iloc[j]
        print('stop is',stop)
        if not i in ANodes:
            print('Node ', i, ' does not exist!')
            break
        if not stop in BNodes:
            print('Node ', stop, ' does not exist!')
            break
        if (i,stop) in dictn_f:
            print('path already in dictionnary')
            for m in dictn_f[i,stop]:                   #pick up the shortest using the final library based on distance
                if len(str(m)) ==5:
                    xtra = {'LINE NAME': df['LINE NAME'][j],'N': m, 'STOP':0}
                    df1 = df1.append(xtra, ignore_index=True) 
                    nextrow = df.iloc[[j]]
                    df1 = df1.append(nextrow,ignore_index=True)
                    break
                for l in m:
                    xtra = {'LINE NAME': df['LINE NAME'][j],'N': l, 'STOP': 0}
                    df1 = df1.append(xtra, ignore_index=True)
                nextrow = df.iloc[[j]]
                df1 = df1.append(nextrow, ignore_index=True)   
                break
            continue
        else:    
            level1 = lib[lib['ANode']==i]
            level1 = level1['BNode'].to_list()
            if stop in level1:
                print('stop reached!')
                nextrow = df.iloc[[j]]
                df1 = df1.append(nextrow, ignore_index=True)
                continue
            PATHS= []
            path1 = [[i,k] for k in level1 if i != k]
            for item in path1:
                level2 = lib[lib['ANode']==item[-1]]
                level2 = level2['BNode'].to_list()
                for k in level2:                   
                    if k == stop: 
                        print('stop reached!')
                        if (i,stop) not in dictn:
                            dictn[i,stop] = []
                        dictn[i,stop].append(item[-1])
                        dis1 = lib[lib['LinkID']== str(i) + '_' + str(item[-1])]
                        dis1 = dis1['Distance_m'].to_list()
                        dis2 = lib[lib['LinkID'] == str(item[-1]) + '_' + str(stop)]
                        dis2 = dis2['Distance_m'].to_list()
                        sum_dis=[]
                        sum_dis.append(dis1)
                        sum_dis.append(dis2)
                        sum_dis = [x for xs in sum_dis for x in xs] # remove brackets from list
                        sum_dis = sum(sum_dis)
                        if (i,stop) not in dictn_d:
                            dictn_d[i,stop]=[] 
                        dictn_d[i,stop].append(sum_dis)
                    if k in level1 or k==i:
                        pass
                    else:                   
                        path2 = [*item,k]
                        PATHS.append(path2)
        if len(PATHS)==0:
            continue
        else:
            for item in PATHS:
                if (i,stop) not in dictn:
                    dictn[i,stop] = []
                if (i,stop) not in dictn_f:
                    dictn_f[i,stop] = []
                if len(dictn_f[i,stop]) != 0:
                    break
                level3 = lib[lib['ANode']==item[-1]]
                level3 = level3['BNode'].to_list()
                for z in level3:
                    if z == stop:
                        print('stop reached!')
                        store_distance = []
                        store_path = []
                        for x in range(len(item)):  
                            if item[x] != item[0]:
                                store_path.append(item[x])
                            if item[x] == item[-1]:
                                dis = lib[lib['LinkID'] == str(item[x]) + '_' + str(stop)]
                                dis = dis['Distance_m'].to_list() 
                                store_distance.append(dis) 
                            else:
                                dis = lib[lib['LinkID'] == str(item[x]) + '_' + str(item[x+1])]
                                dis = dis['Distance_m'].to_list()
                                store_distance.append(dis)     
                        store_distance = [x for xs in store_distance for x in xs] # remove brackets from list
                        store_distance = sum(store_distance)
                        if (i,stop) not in dictn_d:
                            dictn_d[i,stop]=[] 
                        dictn_d[i,stop].append(store_distance)
                        dictn[i,stop].append(store_path)
                    if (i,stop) in dictn:
                        if (i,stop) not in dictn_f:
                            dictn_f[i,stop] = []                         
                        if time.time()-start_node >= 20:
                            print('It has been 30sec, pick up an existing path')
                            if len(dictn[i,stop]) == 1:  
                                for m in dictn[i,stop]:
                                    dictn_f[i,stop]=dictn[i,stop]
                                    if len(str(m)) ==5:
                                        xtra = {'LINE NAME': df['LINE NAME'][j],'N': m, 'STOP':0}
                                        df1 = df1.append(xtra, ignore_index=True) 
                                        nextrow = df.iloc[[j]]
                                        df1 = df1.append(nextrow,ignore_index=True)
                                        break
                                    for l in m:
                                        xtra = {'LINE NAME': df['LINE NAME'][j],'N': l, 'STOP': 0}
                                        df1 = df1.append(xtra, ignore_index=True)
                                    nextrow = df.iloc[[j]]
                                    df1 = df1.append(nextrow, ignore_index=True)
                                    break
                                break                            
                            if len(dictn[i,stop]) == 2:    
                                print('2 paths stored after 30sec')
                                p = min(dictn_d[i,stop])
                                for m,n in zip(dictn_d[i,stop],dictn[i,stop]):                               
                                    if m == p:
                                        dictn_f[i,stop].append(n)
                                        if len(str(n)) ==5:
                                            xtra = {'LINE NAME': df['LINE NAME'][j],'N': n, 'STOP':0}
                                            df1 = df1.append(xtra, ignore_index=True) 
                                            nextrow = df.iloc[[j]]
                                            df1 = df1.append(nextrow,ignore_index=True)
                                            break
                                        for l in n:
                                            xtra = {'LINE NAME': df['LINE NAME'][j],'N': l, 'STOP':0}
                                            df1 = df1.append(xtra, ignore_index=True) 
                                        nextrow = df.iloc[[j]]
                                        df1 = df1.append(nextrow,ignore_index=True)
                                        break
                                break                                    
                        print('number of paths',len(dictn[i,stop]))
                        print(dictn[i,stop])
                        if len(dictn[i,stop]) == Nb_path:                            
                            print('reached 3 paths!')
                            p = min(dictn_d[i,stop])
                            for m,n in zip(dictn_d[i,stop],dictn[i,stop]):                               
                                if m == p:
                                    dictn_f[i,stop].append(n)
                                    if len(str(n)) ==5:
                                        xtra = {'LINE NAME': df['LINE NAME'][j],'N': n, 'STOP':0}
                                        df1 = df1.append(xtra, ignore_index=True) 
                                        nextrow = df.iloc[[j]]
                                        df1 = df1.append(nextrow,ignore_index=True)
                                        break
                                    for l in n:
                                        xtra = {'LINE NAME': df['LINE NAME'][j],'N': l, 'STOP':0}
                                        df1 = df1.append(xtra, ignore_index=True) 
                                    nextrow = df.iloc[[j]]
                                    df1 = df1.append(nextrow,ignore_index=True)
                                    break
                            pass
                    if (i,stop) not in dictn:
                        dictn[i,stop] = []
                    if len(dictn_f[i,stop]) != 0:
                        break                    
                    else:
                        if z in chain(*PATHS):
                            pass                            
                        else:
                            path=[*item,z]
                            PATHS.append(path)
                                            
print('Data processed!')

# Fill the 0s in relevant columns
df1['NNTIME'] = df1['NNTIME'].fillna(0)
df1['DWELL'] = df1['DWELL'].fillna(0)
df1['ACCESS'] = df1['ACCESS'].fillna(0)

# Adding time
df1['Time'] = df1['LINE NAME'].astype("string")

for i in range(len(df1.index)):
    df1['Time'].iloc[i] = str(df1['Time'].iloc[i]).split("_")[2].strip()
 
df1['Time'] = df1['Time'].str.lstrip('0')
df1['Time'] = df1['Time'].replace('','0')     
df1['Time'] = df1['Time'].astype(int)       #Transform data type as integer

# Filter AM, IP, PM, OP
AM = df1.loc[df1['Time'].between(700,959, inclusive=True)]
IP = df1.loc[df1['Time'].between(1000,1559, inclusive=True)]
PM = df1.loc[df1['Time'].between(1600,1859, inclusive=True)]
OP = df1.loc[(df1['Time'] <= 659) | df1['Time'].between(1900,2359, inclusive=True)]

# Export the dataframes
print('Exporting the data...')
df1.to_csv('Output_ALL'+ '.csv', index=False)     
if not AM.empty:
    print('Exporting ' + 'Output_AM' + '.csv...')
    AM.to_csv('Output_AM' + '.csv', index=False)  
if not IP.empty:
    print('Exporting ' + 'Output_IP' + '.csv...')
    IP.to_csv('Output_IP' + '.csv', index=False)  
if not PM.empty:
    print('Exporting ' + 'Output_PM' + '.csv...')
    PM.to_csv('Output_PM' + '.csv', index=False)  
if not OP.empty:
    print('Exporting ' + 'Output_OP' + '.csv...')
    OP.to_csv('Output_OP' + '.csv', index=False)  

Script_time = time.time()-start
    
print("Script finished. It took:", convert(Script_time), "hours, minutes and seconds.")
