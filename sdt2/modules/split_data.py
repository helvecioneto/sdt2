# -*- coding: utf-8 -*-
from modules.load_stations import load_stations
from modules.load_config import load_config
from modules.top_header import top_header
import pandas as pd
from datetime import datetime
import math
import numpy as np

def split_data():
#    main_menu()
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
         
#        try:
        process_meteo(df[meteoH])
#        except:
#            print('Error in Meteorological variables: ', file)
        try:
             solarDF = df[solarH]
        except:
            print('Error in Solarimetric variables: ', file)

        input("\t\t Select Station: ")
    
def header():
    config = load_config()
    header1_ = config[0]['FIX_HEADER']
    header2_ = config[0]['HEADER2']  
    
    header1 = pd.read_excel(header1_)
    header2 = pd.read_excel(header2_,sheet_name='Cabeçalhos', header=None)
    
    solar_header = header2[3:6]
    met_header = header2[22:25].iloc[:,0:12]
    return header1.columns.values, met_header.values, solar_header.values

def process_meteo(meteo):
    
    meteo['timestamp'] = pd.to_datetime(meteo.year, format='%Y') + pd.to_timedelta(meteo.day - 1, unit='d')  + pd.to_timedelta(meteo['min'], unit='m')
    meteo = meteo.set_index('timestamp')
    
    # Conversion types
    conversion = {'id': 'first', 'year': 'first', 'day': 'first', 'min': 'first',
                  'temp_sfc': 'first', 'humid': 'first', 'press': 'first',
                  'prec': 'sum', 'ws10_avg': 'mean', 'wd10_avg': 'median', 'wd10_std': lambda x: np.sqrt(sum(x.values**2))}
    
    #Mask to not resample this values
    Maska = meteo[(meteo != 3333.0) & (meteo != -5555) & (meteo != np.nan)]
    
     #Apply ressample based conversion
    Maska = Maska.resample('10min',how=conversion)
    
    ## Unmask values
    Unmask = meteo[(meteo == 3333.0) & (meteo == -5555) & (meteo == np.nan)].resample('10min',how='first')
    
    ## Combine values
    meteorological = Unmask.combine_first(Maska)
#    
    print(meteo.loc[meteo['temp_sfc'] == 3333.0] )
    
#    print(meteorological[(meteorological == 3333.0)])
    
#    print(Maska.where(Maska.temp_sfc == 'NaN'))
    
#    df3 = pd.merge(Unmask, Maska, how='outer', on=['id', 'year', 'day', 'min', 'temp_sfc', 'humid', 'press', 'prec','ws10_avg', 'wd10_avg', 'wd10_std'])
#    
#    print(Unmask)
    
#    # Final DataFrame
#    meteorological = Maska.fillna(meteo)
    
#    print(Maska)

