# -*- coding: utf-8 -*-
from modules.load_stations import load_stations
from modules.load_config import load_config
from modules.top_header import top_header
import pandas as pd
import errno
import os
import numpy as np
import warnings
warnings.filterwarnings('ignore')

## Global variables
config = load_config()

## Used to copy header
aux = config[0]['MET_HEADER'].copy()
aux2 = config[0]['SOLAR_HEADER'].copy()

meteoH = config[0]['meteoH']
solarH = config[0]['solarH']
BRUTE_HEADER = config[0]['BRUTE_HEADER']
MET_HEADER = config[0]['MET_HEADER']
SOLAR_HEADER = config[0]['SOLAR_HEADER']

try:
    MET_UPDATE = config[0]['MET_UPDATE']
    SOL_UPDATE = config[0]['SOL_UPDATE']
except:
    MET_UPDATE = None
    SOL_UPDATE = None


def split_data():

    top_header('Main Menu > Solarimetric > Split Data')
    print('\t\tPlease select one option\n')
    
    files = load_stations()
    process_files(files)
    
def process_files(files):

    for file in sorted(files):
        df = pd.read_table(file,header=None, sep=',')
        df.columns = BRUTE_HEADER
         
        try:
            process_meteo(df[meteoH].rename(columns=str.lower),file)
        except:
            print('Error in Meteorological variables: ', file)
        try:
            process_solar(df[solarH].rename(columns=str.lower),file)
        except:
            print('Error in Solarimetric variables: ', file)
        ## Continue
        input("Press Enter to Continue:")
        ## Clean screan and print first 20 
        os.system('cls' if os.name == 'nt' else 'clear')
    print("All files are translated!!")
    

def process_meteo(meteo,file):
    config = load_config()
    meteo['timestamp'] = pd.to_datetime(meteo.year, format='%Y') + pd.to_timedelta(meteo.day - 1, unit='d')  + pd.to_timedelta(meteo['min'], unit='m')
    meteo = meteo.set_index('timestamp')
    
    ##for calc ws10_std
    meteo['ws10_std'] = meteo['ws10_avg']
    
    ## Rename columns
    meteo.rename(columns={'day':'jday','temp_sfc':'tp_sfc','prec':'rain'},inplace=True)
    
#    # Conversion types
#    conversion = {'id': 'first', 'year': 'first', 'jday': 'first', 'min': 'first',
#                  'tp_sfc': 'first', 'humid': 'first', 'press': 'first',
#                  'rain': 'sum', 'ws10_avg': 'mean', 'wd10_avg': 'median', 'wd10_std': lambda x: np.sqrt(sum(x.values**2))}
#    
    conversion = {'id': 'first', 'year': 'first', 'jday': 'first', 'min': 'first',
                  'tp_sfc': 'first', 'humid': 'first', 'press': 'first','rain': 'sum',
                  'ws10_avg': 'mean','ws10_std': 'std', 'wd10_avg': 'median', 'wd10_std': 'std'}
    
    #Mask to not resample incorrect values
    Maska = meteo[(meteo != 3333.0) & (meteo != -5555) & (meteo != np.nan)]
    
    
     #Apply ressample based conversion
    Maska = Maska.resample('10min',how=conversion)
    
    ## Unmask values
    Unmask = meteo[(meteo == 3333.0)].resample('10min').first()
    Unmask2 = meteo[(meteo == -5555)].resample('10min').first()
    
    ## Combine values
    meteorological = Unmask.combine_first(Maska)
    meteorological = Unmask2.combine_first(meteorological)
    
    ## Reset index
    meteorological = meteorological.reset_index()
    # Realocate order of columns
    meteorological = meteorological.reindex(columns=['id','timestamp','year','jday','min',
                                                     'tp_sfc','humid','press','rain',
                                                     'ws10_avg','ws10_std','wd10_avg','wd10_std'])
            
    ## Change type of columns
    meteorological[['id','year','jday','min']] = meteorological[['id','year','jday','min']].astype(int)

    ## Header check
    file_ = file.name
    year_ = file.parent.name
    ## Extract month string
    month = (meteorological['timestamp'][0].strftime('%m'))
    stat_ = file.parent.parent.name
    header_path = config[0]['LOG_HEADER']+stat_+'/'+year_+'/'+file_[:-4]+'.met_head'   
    output = config[0]['OUTPUT']+stat_+'/'+year_+'/'+stat_[:-3]+'_'+year_+'_'+month+'_MD_formatado.csv' 
     
    

    ### Create dir of headers if not exist
    if not os.path.exists(os.path.dirname(header_path)):
        try:
            os.makedirs(os.path.dirname(header_path))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
                
        ### Create dir of output if not exist
    if not os.path.exists(os.path.dirname(output)):
        try:
            os.makedirs(os.path.dirname(output))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
 
    ## Update Global MET_HEADER
    global MET_HEADER
    # For key in MET_UPDATE Check    
    met1 = []
    met2 = []

    if MET_UPDATE != None:
        for k in MET_UPDATE:   
            if len(meteorological.loc[meteorological['timestamp'] >= k[0]]) > 0:
                for kk in k:
                    # Update Global variable
                    for idx, item in enumerate(MET_HEADER):
                        if kk[0] in  item[0]:
                            MET_HEADER[idx] = kk

                # Separete files
                met1 = meteorological.loc[meteorological['timestamp'] >= k[0]]
                # Create Multindex based in columns from header_log
                mux = pd.MultiIndex.from_tuples(MET_HEADER)
                # Fix multindex on dataframe
                met1.columns = mux
                
                ## Second file
                met2 = meteorological.loc[meteorological['timestamp'] < k[0]]
                mux2 = pd.MultiIndex.from_tuples(aux)
                met2.columns = mux2
            
    # Create Multindex based in columns from header_log
    mux = pd.MultiIndex.from_tuples(MET_HEADER)
            # Fix multindex on dataframe
    meteorological.columns = mux
                
                         
    if len(met1) == len(meteorological):
        # Clean screan and print first 20 
        print('Processing File -> ',file)
        print('\nSplit weather data!: ')
        print('aqui')
        print(meteorological)
        meteorological.to_csv(output,index=False)
    if (len(met1) and len(met2)) > 0:
        # Clean screan and print first 20 
        ## Save files
        print('\nSplit weather data!: ',output)
        print(met2.head(),'\n')
        print('\nSplit weather data!: ',output[:-4]+str('_02.csv'))
        print(met1.tail())
        met1.to_csv(output[:-4]+str('_02.csv'),index=False)
        met2.to_csv(output,index=False)
    else:
        # Clean screan and print first 20 
        print('Processing File -> ',file)
        print('\nSplit weather data!: ',output)
        print(meteorological)
        meteorological.to_csv(output,index=False)


def process_solar(solar,file):

    config = load_config()
    ## Create Timestamp    
    solar['timestamp'] = pd.to_datetime(solar.year, format='%Y') + pd.to_timedelta(solar.day - 1, unit='d')  + pd.to_timedelta(solar['min'], unit='m')

    # Change position of timestamp    
    cols = list(solar)
    cols.insert(1, cols.pop(cols.index('timestamp')))
    solar = solar.ix[:, cols]

    
    ## Header check
    file_ = file.name
    year_ = file.parent.name
    month = (solar['timestamp'][0].strftime('%m'))
    stat_ = file.parent.parent.name
    header_path = config[0]['LOG_HEADER']+stat_+'/'+year_+'/'+file_[:-4]+'.solar_head'  
    output = config[0]['OUTPUT']+stat_+'/'+year_+'/'+stat_[:-3]+'_'+year_+'_'+month+'_SD_formatado.csv' 
    
    ### Create dir of headers if not exist
    if not os.path.exists(os.path.dirname(header_path)):
        try:
            os.makedirs(os.path.dirname(header_path))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
                
    ## Update Global SOLAR_HEADER
    global SOL_UPDATE
    # For key in SOLAR_UPDATE Check    
    sol1 = []
    sol2 = []

    if SOL_UPDATE != None:
        for k in SOL_UPDATE:   
            if len(solar.loc[solar['timestamp'] >= k[0]]) > 0:
                for kk in k:
                    # Update Global variable
                    for idx, item in enumerate(SOLAR_HEADER):
                        if kk[0] in  item[0]:
                            SOLAR_HEADER[idx] = kk

                # Separete files
                sol1 = solar.loc[solar['timestamp'] >= k[0]]
                # Create Multindex based in columns from header_log
                mux = pd.MultiIndex.from_tuples(SOLAR_HEADER)
                # Fix multindex on dataframe
                sol1.columns = mux
                
                ## Second file
                sol2 = solar.loc[solar['timestamp'] < k[0]]
                mux2 = pd.MultiIndex.from_tuples(aux2)
                sol2.columns = mux2
            
    # Create Multindex based in columns from header_log
    mux = pd.MultiIndex.from_tuples(SOLAR_HEADER)
            # Fix multindex on dataframe
    solar.columns = mux
                
                         
    if len(sol1) == len(solar):
        # Clean screan and print first 20 
        print('Processing File -> ',file)
        print('\nSplit weather data!: ')
        print('aqui')
        print(solar)
        solar.to_csv(output,index=False)
    if (len(sol1) and len(sol2)) > 0:
        # Clean screan and print first 20 
        ## Save files
        print('\nSplit weather data!: ',output)
        print(sol2.head(),'\n')
        print('\nSplit weather data!: ',output[:-4]+str('_02.csv'))
        print(sol1.tail())
        sol1.to_csv(output[:-4]+str('_02.csv'),index=False)
        sol2.to_csv(output,index=False)
    else:
        # Clean screan and print first 20 
        print('Processing File -> ',file)
        print('\nSplit weather data!: ',output)
        print(solar)
        solar.to_csv(output,index=False)
