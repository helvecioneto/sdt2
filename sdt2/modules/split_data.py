# -*- coding: utf-8 -*-
from modules.load_stations import load_stations
from modules.load_config import load_config
from modules.top_header import top_header
import pandas as pd
import errno
import os
import json
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def split_data():

    top_header('Main Menu > Solarimetric > Split Data')
    print('\t\tPlease select one option\n')
    
    files = load_stations()
    process_files(files)
    
def process_files(files):
    headers = header()
    for file in sorted(files):
        df = pd.read_table(file,header=None, sep=',')
        df.columns = headers[0]
        df = df.rename(columns=str.lower)

        meteoH = ['id', 'year', 'day', 'min', 'temp_sfc', 'humid',
                'press', 'prec', 'ws10_avg', 'wd10_avg', 'wd10_std']
        
        solarH =    ['id','year','day','min','global_avg','global_std','global_max','global_min',
                    'diffuse_avg','diffuse_std','diffuse_max','diffuse_min','par_avg','par_std','par_max',
                    'par_min','lux_avg','lux_std','lux_max','lux_min','direct_avg','direct_std',
                    'direct_max','direct_min','lw_avg','lw_std','lw_max','lw_min','temp_global',
                    'temp_direct','temp_diffuse','temp_dome','temp_case']
         
        try:
            process_meteo(df[meteoH],file)
        except:
            print('Error in Meteorological variables: ', file)
        try:
            process_solar(df[solarH],file)
        except:
            print('Error in Solarimetric variables: ', file)
        ## Continue
        input("Press Enter to Continue:")
        ## Clean screan and print first 20 
        os.system('cls' if os.name == 'nt' else 'clear')
    print("All files are translated!!")
    
def header():
    config = load_config()
    header1_ = config[0]['FIX_HEADER']
    header2_ = config[0]['HEADER2']  
    
    header1 = pd.read_excel(header1_)
    header2 = pd.read_excel(header2_,sheet_name='Cabeçalhos', header=None)
    
    solar_header = header2[3:6]
    met_header = header2[22:25].iloc[:,0:12]
    return header1.columns.values, met_header.values, solar_header.values

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

    ## If file exist
    try:
        with open(header_path) as f:
            column = json.load(f)
            columns = column['MET_HEADER']
        # Do something with the file
    except IOError:
        ## Write file
        with open(header_path, 'w+') as file:
            file.write(json.dumps(dict( MET_HEADER = config[0]['MET_HEADER'])))
            columns = config[0]['MET_HEADER']
            
            
    ## Remove timestamp
    meteorological = meteorological.drop(columns='timestamp')
     
    # Create Multindex based in columns from header_log
    mux = pd.MultiIndex.from_tuples(columns)

    # Fix multindex on dataframe
    meteorological.columns = mux
    
    
    ### Create dir of output if not exist
    if not os.path.exists(os.path.dirname(output)):
        try:
            os.makedirs(os.path.dirname(output))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    ## Clean screan and print first 20 
    os.system('cls' if os.name == 'nt' else 'clear')
    print('Processing File -> ',file)
    print('\nSplit weather data!: ')
    print(meteorological.head(20))
    
    ## Save file
    meteorological.to_csv(output,index=False)

def process_solar(solar,file):

    config = load_config()
    ## Create Timestamp    
    solar['timestamp'] = pd.to_datetime(solar.year, format='%Y') + pd.to_timedelta(solar.day - 1, unit='d')  + pd.to_timedelta(solar['min'], unit='m')

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
    ## If file exist
    try:
        with open(header_path) as f:
            column = json.load(f)
            columns = column['SOLAR_HEADER']
        # Do something with the file
    except:
        ## Write file
        with open(header_path, 'w+') as file:
            file.write(json.dumps(dict( SOLAR_HEADER = config[0]['SOLAR_HEADER'])))
            columns = config[0]['SOLAR_HEADER']
            
    ## Remove timestamp
    solar = solar.drop(columns='timestamp')
    
    # Create Multindex based in columns from header_log
    mux = pd.MultiIndex.from_tuples(columns)

    
    # Fix multindex on dataframe
    solar.columns = mux
    
    
    ### Create dir of output if not exist
    if not os.path.exists(os.path.dirname(output)):
        try:
            os.makedirs(os.path.dirname(output))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    ## Clean screan and print first 20 
    print('\nSplit solar data! :')
    print(solar.head(20))
    
    ## Save file
    solar.to_csv(output,index=False)