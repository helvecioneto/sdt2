# -*- coding: utf-8 -*-
from modules.top_header import top_header
from modules.load_config import load_config
from os import listdir
import pandas as pd
import numpy as np

def historic_generate():
    
    operation_dir = load_config()[0]['OPERATIONAL_IN']
    
    top_header('Main > Preprocessing > Generate Historical')
    print('\t\tPlease select one stations to generate historical data: ')
    
    operational_stations = [fn for fn in listdir(operation_dir)]

    count = -1
    for f in operational_stations:
        count = count + 1
        print ("\t\t [%s]"  % count + f)
        
    while True:
        try:
            ans_file = int(input("\t\t Select Station: "))
        except:
            print("\t\t Wrong selection")
            continue
        if ans_file > count:
            print ("\t\t Wrong selection.")
            continue
        
        selected_st = operation_dir + operational_stations[ans_file] + '/'
        break
    
    ## SELECT TYPE OF DATA
    top_header('Main > Preprocessing > Generate Historical > '+str(operational_stations[ans_file]).upper())
    print('\t\tPlease select type of data to generate historical data: ')
    
    dataTypes = ['MD','SD','TD','50','25','10']
    
    countT = -1
    for f in dataTypes:
        countT = countT + 1
        print ("\t\t [%s]"  % countT + f)
        
    while True:
        try:
            ans_type = int(input("\t\t Select Station: "))
        except:
            print("\t\t Wrong selection")
            continue
        if ans_type > countT:
            print ("\t\t Wrong selection.")
            continue
        
        selected_file = operational_stations[ans_file].upper() + '_' + str(dataTypes[ans_type]) + '.DAT'
        break
    
    ### DATA TYPES
    if operational_stations[ans_file] == 'sms':
        
        ## OPEN DATA
        df = pd.read_csv(selected_st+selected_file, sep=",", header=None, skiprows=4,skipinitialspace=False)
        df1 = df.copy()
        head0 = pd.read_csv(selected_st+selected_file,sep=",",header=None, nrows = 1)
        head1 = pd.read_csv(selected_st+selected_file,sep=",",header=None, skiprows=1,nrows = 1)
        head2 = pd.read_csv(selected_st+selected_file,sep=",",header=None, skiprows=3,nrows = 1)
        
        head0 = head0.iloc[0].values
        head1 = head1.iloc[0].values
        head2 = head2.iloc[0].values

        
        df1[0] = pd.to_datetime(df1[0] , format='%Y-%m-%d %H:%M:%S')
        
        ## SELECT TIMESTAMP TO PROCESSE
        top_header('Main > Preprocessing > Generate Historical > '+str(operational_stations[ans_file]).upper() + ' > '+ str(dataTypes[ans_type]))
        print('\t\tPlease select type of data to generate historical data: ')
        
        ## AVAIBLE YEARS
        years = df1[0].dt.year.unique()
        countY = -1
        for f in years:
            countY = countY + 1
            print ("\t\t [%s]"  % countY + str(f))
        while True:
            try:
                ans_year = int(input("\t\t Select Year: "))
            except:
                print("\t\t Wrong selection")
                continue
            if ans_year > countY:
                print ("\t\t Wrong selection.")
                continue
            
            selected_year = years[ans_year]
            break

        df1 = df1.set_index(0)

        months = df1.loc[str(selected_year)]
        months = months.reset_index()
        months = months[0].dt.strftime('%m').unique()
        
        top_header('Main > Preprocessing > Generate Historical > '+str(operational_stations[ans_file]).upper() + ' > ' + str(dataTypes[ans_type]) +' > '+ str(selected_year))
        print('\t\tPlease select type of data to generate historical data: ')
        
        countM = -1
        for f in months:
            countM = countM + 1
            print ("\t\t [%s]"  % countM + str(f))
        while True:
            try:
                ans_month = int(input("\t\t Select Month: "))
            except:
                print("\t\t Wrong selection")
                continue
            if ans_month > countM:
                print ("\t\t Wrong selection.")
                continue
            
            selected_month = months[ans_month]
            break
        
        top_header('Main > Preprocessing > Generate Historical > '+str(operational_stations[ans_file]).upper() + ' > ' + str(dataTypes[ans_type]) +' > '+ str(selected_year) +' > '+ str(selected_month))

        #SELECTED TO GENERATE
        df1 = df1.loc[str(selected_year)+'-'+str(selected_month)]
        
        ## TIME INTERVAL VERIFICATION
        df1 = df1.sort_index(ascending=True)
        
        # GET TIMES STRING
        max_time = df1.index.max()
        min_time = df1.index.min()
       
        ## FIND INDEX INTO ORIGINAL DATAFRAME
        idx_min = df.loc[df[0] == str(min_time)].index.values[0]
        idx_max = df.loc[df[0] == str(max_time)].index.values[0]
        
        ## LOC BETWEEN IDX
        locked_df_chk = df.loc[idx_min:idx_max]
        
        ## FINAL DF TO COMPARE
        final_df = locked_df_chk.copy()
        
        ## MOUNT INDEX OF DATES ACORDING TYPE OF DATA
        if dataTypes[ans_type] == 'MD' or dataTypes[ans_type] == '10' or dataTypes[ans_type] == '25' or dataTypes[ans_type] == '50':
            freqc = '10min'
        if dataTypes[ans_type] == 'SD' or dataTypes[ans_type] == 'TD':
            freqc = '1min'
        ## PASS COLUMN TO DATETIME    
        locked_df_chk[0] = pd.to_datetime(locked_df_chk[0] , format='%Y-%m-%d %H:%M:%S')
        
        ##REINDX TO DETECT
        try:
            locked_df_chk = locked_df_chk.set_index(0).asfreq(freqc)
            
            if len(locked_df_chk) == len(final_df):
                print(final_df)
            else:
                print('Arquivo reprovado')
                print('Total de linhas vazias :',locked_df_chk[1].isna().sum())
                emptyrows = locked_df_chk[1].index[locked_df_chk[1].apply(np.isnan)]
                print('Linhas vazias: ',emptyrows)
                print(locked_df_chk)
        except:
            print('Arquivo reprovado')
            locked_df_chk = locked_df_chk.set_index(0)
            duplicated_lines = locked_df_chk[locked_df_chk.index.duplicated()]
            print('Linhas duplicadas : ',duplicated_lines)
            
       