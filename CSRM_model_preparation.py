# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 15:35:01 2022

@author: SCHN9446
email: charlotte.schnoebelen@atkinsglobal.com

This script has been developed for the purposes of preparing the necessary inputs for the CSRM model visualisation platform.
It processes data extracted using SATURN modules, and converts them to georeferenced and named JSON objects.
A lookup csv file is used to point to the corresponding data of each scenario, year and time period.
"""


#%% user input
#This section defines any necessary inputs and paths not captured in the lookup.csv file
##model_directory = "./Data_dump/" #name of the subfolder containing the model files

#%% Import dependencies
import pandas as pd
import json
import os
import numpy as np
import gc

#%%BLUE specific
#Import a list of in-scope zones to filter the OD data by to avoid memory issues when running the script
zonesdf=pd.read_csv("cordon_zones.csv",header=None,names=['ZoneID'], dtype=np.uint32) #"str")
cordon_zones=zonesdf['ZoneID'].values.tolist()

#Import the zone to sector correspondence for the OD data preparation
# corr=pd.read_csv('zone_sector.csv',dtype={"ZoneID":np.uint32})#{"ZoneID":"str"})
corr=pd.read_csv('zone_sector.csv',dtype={"ZoneID":np.uint32, "SectorID":np.uint8})
del corr["SectorName"]

#%%user functions
#function that read P1Xdump CSV files
def read_link(csvfile):
    temp=pd.read_csv(csvfile,header=None,index_col=False,
                   names=['ANode','BNode','CNode','distance','aflow','dflow','ffspeed', 'nspeed', 'delay', 'vc','avgqueuetotal'],
                          dtype={'ANode':'str','BNode':'str','CNode':'str'})
    temp=temp[temp.CNode.isnull()]
    temp['LinkID']=temp.ANode.map(str)+'_'+temp.BNode.map(str)
    temp.drop(['ANode','BNode','CNode'],axis=1,inplace=True)
    temp.set_index('LinkID',inplace=True)
    return temp

#function that read node data from SATBD outputs
def read_node(csvfile):
    temp=pd.read_csv(csvfile,sep='\s+',header=None,names=['NodeID','delay','vc','NodeType'],
             dtype={'NodeID':'str'}).set_index('NodeID')
    temp.NodeType=temp.NodeType.map(junction_types) #get junction type
    temp.vc=np.where(temp.vc=='m',0.0,temp.vc)
    temp.vc=temp.vc.map(float)
    return temp

#function to read network shapefiles
def read_shp(shpfile):
    with open(shpfile, 'r') as f:
     temp = json.load(f)
    return temp

#function to read TUBA3-format model skims
def read_skims(path,var_name):
    print('Reading skim: ' + idx_to_var[var_name])
    
    # temp=pd.read_csv(path,header=None,sep='\t')
    # temp=temp.loc[:,0].str.split(expand=True) #split on tab
    # temp.info(memory_usage="deep")
    # temp.columns=['o','d','uc','value']
    # temp['variable']=var_name
    # temp['value']=pd.to_numeric(temp['value'])
    
    # temp=pd.read_csv(path, delimiter=r"\s+", header=None, names=['o', 'd', 'uc' ,'value'], dtype={'o':np.uint32, 'd':np.uint32, 'uc':np.uint8, 'value':np.float64}, sep='\t', nrows=1000000)
    temp=pd.read_csv(path, delimiter=r"\s+", header=None, names=['o', 'd', 'uc' ,'value'], dtype={'o':np.uint32, 'd':np.uint32, 'uc':np.uint8, 'value':np.float64})
    temp=temp.loc[(temp["o"].isin(cordon_zones))&(temp["d"].isin(cordon_zones))]
    temp['variable']=var_name
    return temp

#function to read SLA files (produced using SATURN's SLA basket functionality)
def read_SLA(SLAfile,sla_links_list):
    temp=pd.read_fwf(SLAfile,widths=[10]*(3+len(sla_links_list)),header=None,names=['ANode','BNode','CNode']+sla_links_list)
    temp[['ANode','BNode','CNode']]=temp[['ANode','BNode','CNode']].applymap(lambda x: str(x).replace('  ',''))
    temp=temp[temp.CNode=='nan'] #drop all turns
    temp['LinkID']=temp['ANode']+'_'+temp['BNode']
    temp.drop(columns=['ANode','BNode','CNode'],inplace=True)
    temp['highlight']=np.where(temp.LinkID.isin(sla_links_list),1,0) #this will help identify SLA links in the visualisation platform
    temp.set_index('LinkID',inplace=True)
    return temp

#function to extract zone trip totals data from SATURN Total files (.TOT).
def read_totals(csvfile):
    zone_data=pd.read_csv(csvfile,names=['totals_text'])
    zone_data['level']=zone_data.totals_text.str.extract(r'TOTALS FOR LEVEL\s+(\d+) OF THE INTERNAL') #extract userclass id using a regular expression
    zone_data.loc[zone_data.totals_text.str.contains('SUMMED OVER ALL LEVELS'),'level']='tot'
    zone_data['level']=zone_data['level'].fillna(method='ffill')
    zone_data.dropna(inplace=True)
    zone_data=zone_data.iloc[:-13].groupby('level').apply(lambda x:x.iloc[4:-4]) #only keep totals
    zone_data.set_index('level',inplace=True)
    zone_data=zone_data.totals_text.str.strip()
    #zone_data=zone_data.str.replace('.', ' ') #remove full stops
    zone_data=zone_data.str.split('\s+',expand=True) #split on whitespace
    zone_data=zone_data.iloc[:,1:4] #moved from line 84 below
    zone_data=zone_data.astype(np.float64)
    zone_data.columns=['ZoneID','trips_from','trips_to']
    zone_data['trips_from']=round(zone_data.trips_from)
    zone_data['trips_to']=round(zone_data.trips_to)
    zone_data=zone_data.astype(int)
    zone_data.index='UC'+zone_data.index
    zone_data=zone_data.set_index(['ZoneID',zone_data.index]).unstack()
    zone_data.columns=zone_data.columns.map('_'.join)
    zone_data=zone_data[['trips_from_UCtot','trips_to_UCtot']]
    return zone_data

os.chdir(os.getcwd())
#%%lookups
#read lookup file pointing to the appropriate file locations, and construct some additional output filenames
lookup=pd.read_csv('lookup.csv')
lookup=lookup.loc[lookup['new_run'] == 1].reset_index()
lookup['csv_file_links']=lookup.assignment_filename.str.replace('\.UFS|\.ufs','_LINKS.csv')
lookup['output_filename_links']='link_'+lookup.scenario.map(str)+'_'+lookup.year.map(str)+'_'+\
        lookup.time_period.map(str)+'.js'

lookup['csv_file_nodes']=lookup.assignment_filename.str.replace('\.UFS|\.ufs','_NODES.TXT')
lookup['output_filename_nodes']='node_'+lookup.scenario.map(str)+'_'+lookup.year.map(str)+'_'+\
        lookup.time_period.map(str)+'.js'

lookup['csv_file_zones']=lookup.matrix_filename.str.replace('\.UFM|\.ufm','.TOT')
lookup['output_filename_zones']='zone_'+lookup.scenario.map(str)+'_'+lookup.year.map(str)+'_'+\
        lookup.time_period.map(str)+'.js'

lookup['csv_file_SLA']=lookup.assignment_filename.str.replace('\.UFS|\.ufs','_SLA.TXT')
lookup['output_filename_SLA']='SLA_'+lookup.scenario.map(str)+'_'+lookup.year.map(str)+'_'+\
        lookup.time_period.map(str)+'.js'

lookup['model_directory']=lookup.filepath.map(str)+r'\\'

junction_types={'m':'Buffer','0':'External node','1':'Priority junction','2':'Roundabout','3':'Traffic signal',
                '4':'Dummy node','5':'Roundabout'} #Junction type lookup between SATDB junction codes and their corresponding description.

# #%% link files
# #This section reads all link-based data extracted from SATURN, merges with the corresponding network shapefile, and exports the produced geojson as named JS objects.
for i in range(lookup.shape[0]):
    params=lookup.loc[i]
    csvfile=params['csv_file_links']
    shpfile=params['shapefile_links']
    year=params['year']
    tp=params['time_period']
    scenario=params['scenario']
    output_filename=params['output_filename_links']
    model_directory=params['model_directory']
    link_data=read_link(model_directory+csvfile)
    link_data=link_data[~(link_data.index.str.contains('C'))] #remove all centroid connectors
    shp=read_shp(shpfile) #read the network shapefile
    #enrich the shapefile with transport model data
    for feature in shp['features']:
        feature['geometry']['coordinates']=np.round(feature['geometry']['coordinates'],decimals=5).tolist() #simplify coordinates
        for var in ['distance','aflow','dflow','ffspeed', 'nspeed', 'delay', 'vc','avgqueuetotal']:
            #feature['properties'][var+'_diff']=0 #placeholder for layer comparison
            try:
                feature['properties'][var]=np.round(link_data.loc[feature['properties']['LinkID']][var],3)
            except:
                feature['properties'][var]=0
    #export as a .js file to the data folder
    with open('../data/'+output_filename, "w") as text_file:
        text_file.write('var link_data_'+scenario+'_'+str(year)+'_'+tp+' = '+ json.dumps(shp))
    #export as geojson
    #with open('../data/'+output_filename.replace('.js','.geojson'), "w") as text_file:
    # d   text_file.write(json.dumps(shp))

#%% node files
#This section reads all node-based data extracted from SATURN, merges with the corresponding network shapefile, and exports the produced geojson as named JS objects.
for i in range(lookup.shape[0]):
    params=lookup.loc[i]
    csvfile=params['csv_file_nodes']
    shpfile=params['shapefile_nodes']
    year=params['year']
    tp=params['time_period']
    scenario=params['scenario']
    output_filename=params['output_filename_nodes']
    model_directory=params['model_directory']
    node_data=read_node(model_directory+csvfile)
    shp=read_shp(shpfile) #read the node shapefile

    for feature in shp['features']:
        feature['geometry']['coordinates'] = np.round(feature['geometry']['coordinates'],decimals=5).tolist() #simplify coordinates
        feature['properties']['NodeID'] = str(feature['properties']['NodeID'])
        for var in ['delay', 'vc','NodeType']:
            #feature['properties'][var+'_diff']=0 #placeholder for layer comparison
            try:
                feature['properties'][var]=node_data.loc[str(feature['properties']['NodeID'])][var]
            except:
                feature['properties'][var]=0
    #export
    with open('../data/'+output_filename, "w") as text_file:
        text_file.write('var node_data_'+scenario+'_'+str(year)+'_'+tp+' = '+ json.dumps(shp))


#%% zone files
#This section reads zone-based data extracted from SATURN, merges with the corresponding network shapefile, and exports the produced geojson as named JS objects.
for i in range(lookup.shape[0]):
    params=lookup.loc[i]
    csvfile=params['csv_file_zones']
    shpfile=params['shapefile_zones']
    year=params['year']
    tp=params['time_period']
    scenario=params['scenario']
    output_filename=params['output_filename_zones']
    model_directory=params['model_directory']
    zone_data=read_totals(model_directory+csvfile)
    shp=read_shp(shpfile)
    for feature in shp['features']:
        #feature['geometry']['coordinates']=[[np.round(x,5).tolist() for x in feature['geometry']['coordinates'][0]]] #simplify coordinates, enable multipart features
        for var in zone_data.columns:
            #feature['properties'][var+'_diff']=0 #placeholder for layer comparison
            try:
                feature['properties'][var]=int(zone_data.loc[feature['properties']['ZoneID']][var])
            except:
                feature['properties'][var]=0
    with open('../data/'+output_filename, "w") as text_file:
        text_file.write('var zone_data_'+scenario+'_'+str(year)+'_'+tp+' = '+ json.dumps(shp))

#%%Zone Centroids
#This section simply converts zone centroid shapefiles (in geojson format) to named JS objects in the "data" folder.
for i in range(lookup.shape[0]):
    params=lookup.loc[i]
    shpfile=params['shapefile_centroids']
    year=params['year']
    tp=params['time_period']
    scenario=params['scenario']
    shp=read_shp(shpfile)
    with open('../data/zoneCentroid_'+str(scenario)+'_'+str(year)+'_'+str(tp)+'.js' , "w") as text_file:
        text_file.write('var zoneCentroid_data_'+scenario+'_'+str(year)+'_'+tp+' = '+ json.dumps(shp))

#%%Zone Connectors
#This section simply converts zone connector shapefiles (in geojson format) to named JS objects in the "data" folder.
for i in range(lookup.shape[0]):
    params=lookup.loc[i]
    shpfile=params['shapefile_connectors']
    year=params['year']
    tp=params['time_period']
    scenario=params['scenario']
    shp=read_shp(shpfile)
    with open('../data/zoneConnector_'+str(scenario)+'_'+str(year)+'_'+str(tp)+'.js' , "w") as text_file:
        text_file.write('var zoneConnector_data_'+scenario+'_'+str(year)+'_'+tp+' = '+ json.dumps(shp))

#%%Sectors
#This section reads sector shapefile data and exports as .js
for i in range(lookup.shape[0]):
    #prepare filename
    params=lookup.loc[i]
    year=params['year']
    tp=params['time_period']
    scenario=params['scenario']
    shpfile=params['shapefile_sectors']
    shp=read_shp(shpfile)
    with open('../data/sectors_'+str(scenario)+'_'+str(year)+'_'+str(tp)+'.js' , "w") as text_file:
        text_file.write('var sectors_data_'+scenario+'_'+str(year)+'_'+tp+' = '+ json.dumps(shp))

#%%OD files
#This section reads OD data (TUBA 3-format skims) extracted from SATURN, merges with the corresponding network shapefile, and exports the produced geojson as named JS objects.
idx_to_var={np.uint8(1):'V', np.uint8(2):'T', np.uint8(3):'D'}
var_to_idx={v: k for k, v in idx_to_var.items()}
common_dtypes={'o':np.uint32, 'd':np.uint32, 'uc':np.uint8}

for i in range(lookup.shape[0]):
    #prepare filename
    params=lookup.loc[i]
    trips_filename=params.matrix_filename.upper().replace('.UFM','_V.txt')
    distSkim_filename=params.assignment_filename.upper().replace('.UFS','_D_Km.TXT')
    timeSkim_filename=params.assignment_filename.upper().replace('.UFS','_T_Hr.TXT')
    output_filename=params.output_filename_links.replace('link_','OD_')
    year=params['year']
    tp=params['time_period']
    scenario=params['scenario']
    model_directory=params['model_directory']
    
    print('Processing:' + ' - '.join([scenario, str(year), tp]))

    #read skims
    trips=read_skims(model_directory+trips_filename, var_to_idx['V'])
    timeSkim=read_skims(model_directory+timeSkim_filename, var_to_idx['T'])
    distSkim=read_skims(model_directory+distSkim_filename, var_to_idx['D'])
    # trips=read_skims(model_directory+trips_filename,'V')
    # timeSkim=read_skims(model_directory+timeSkim_filename,'T')
    # distSkim=read_skims(model_directory+distSkim_filename,'D')

    #filter to only include in-scope OD pairs for memory saving purposes
    # trips=trips.loc[(trips["o"].isin(cordon_zones))&(trips["d"].isin(cordon_zones))]
    # timeSkim=timeSkim.loc[(timeSkim["o"].isin(cordon_zones))&(timeSkim["d"].isin(cordon_zones))]
    # distSkim=distSkim.loc[(distSkim["o"].isin(cordon_zones))&(distSkim["d"].isin(cordon_zones))]

    skims=pd.concat([trips,timeSkim,distSkim])
    del [[trips,timeSkim,distSkim]]
    gc.collect()
    # skims.to_csv('checks.csv', index=False)
    
    print('Attach sector correspondence to aggregate up')
    
    skims.set_index(['o','d','uc','variable'], inplace=True)
    skims=skims.unstack('variable').reset_index().dropna()
    skims.columns=['o','d','uc','V','T','D']
    
    # skims=pd.pivot_table(skims,index=['o','d','uc'],values='value',columns='variable').reset_index().dropna()
    
    skims=skims.astype(common_dtypes)
    skims=skims.merge(corr,left_on='o',right_on='ZoneID',how='left')
    skims=skims.merge(corr,left_on='d',right_on='ZoneID',how='left',suffixes=('_o','_d'))
    #create demand weighted skims for aggregation
    
    print('Create demand weighted skims for aggregation')
    
    skims.rename(columns=idx_to_var, inplace=True)
    skims['D_Weighted']=skims['D'] * skims['V']
    skims['T_Weighted']=skims['T'] * skims['V']
    
    print('Aggregate to sector system')
    
    skims=skims.groupby(['SectorID_o','SectorID_d'])[['V','D_Weighted','T_Weighted']].sum().reset_index()
    skims['D']=skims['D_Weighted'] / skims['V']
    skims['T']=skims['T_Weighted'] / skims['V']
    skims.rename(columns={'SectorID_o':'o','SectorID_d':'d'}, inplace=True)
    skims['V']=round(skims['V'],1)
    skims['D']=round(skims['D'],1)
    skims['T']=round(skims['T'],1)
    skims=skims[['o','d','V','D','T']]
    skims=skims.astype({'o':np.uint8, 'd':np.uint8})

    print('Produce nested dictionaries')
    #produce nested dictionaries
    trips_from=pd.pivot_table(skims,index='o',columns='d',values='V',fill_value=0).to_json(orient='index')
    trips_to=pd.pivot_table(skims,index='d',columns='o',values='V',fill_value=0).to_json(orient='index')
    time_from=pd.pivot_table(skims,index='o',columns='d',values='T',fill_value=0).to_json(orient='index')
    time_to=pd.pivot_table(skims,index='d',columns='o',values='T',fill_value=0).to_json(orient='index')
    dist_from=pd.pivot_table(skims,index='o',columns='d',values='D',fill_value=0).to_json(orient='index')
    dist_to=pd.pivot_table(skims,index='d',columns='o',values='D',fill_value=0).to_json(orient='index')

    print('Export')
    #export
    f = open('../data/'+output_filename,"w")
    f.write('var OD_data_'+scenario+'_'+str(year)+'_'+tp+' = {\'trips_from\':'+str(trips_from).replace('"',"'")+\
                                 ',\'trips_to\':'+str(trips_to).replace('"',"'")+\
                                 ',\'time_from\':'+str(time_from).replace('"',"'")+\
                                 ',\'time_to\':'+str(time_to).replace('"',"'")+\
                                  ',\'dist_from\':'+str(dist_from).replace('"',"'")+\
                                 ',\'dist_to\':'+str(dist_to).replace('"',"'")+'}')
    f.close()
    
    del [[skims, trips_from, trips_to, time_from, time_to, dist_from, dist_to]]
    gc.collect()