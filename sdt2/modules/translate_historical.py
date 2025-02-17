# -*- coding: utf-8 -*-
from modules.top_header import top_header
from modules.load_config import load_config
from dependecies import *

def translate_historical():
    
    operation_dir = load_config()[0]['HISTORICAL_OUT']
    
    top_header('Main > Preprocessing > Translate Historical')
    print('\t\tPlease select one stations to translate historical data: ')
    
    historical_pats = [fn for fn in listdir(operation_dir) if not fn.startswith('.')]
    
    if len(historical_pats) == 0:
        print('There is no data to be formatted')
        input('Press Enter to return')
    ## SELECT STATION
    count = -1
    for f in historical_pats:
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
        
        selected_st = operation_dir + historical_pats[ans_file] + '/'
        top_header('Main > Preprocessing > Translate Historical > '+str(historical_pats[ans_file]))
        print('\t\tPlease select one year file: ')
        
        select_year = [fn for fn in listdir(selected_st) if not fn.startswith('.')]
        
        ## SELECT FILE
        count = -1
        for f in select_year:
            count = count + 1
            print ("\t\t [%s]"  % count + f)
            
        while True:
            try:
                ans_year = int(input("\t\t Select Station: "))
            except:
                print("\t\t Wrong selection")
                continue
            if ans_year > count:
                print ("\t\t Wrong selection.")
                continue
            
            selected_year = selected_st + select_year[ans_year] + '/'
            top_header('Main > Preprocessing > Translate Historical > '+str(historical_pats[ans_file]) +' > ' +select_year[ans_year])
            print('\t\tPlease select one file to translate  file: ')
            
            select_files = [fn for fn in listdir(selected_year) if not fn.startswith('.') and '.dat' in  fn]
            
            ## FOR FILE
            count = -1
            for f in select_files:
                count = count + 1
                print ("\t\t [%s]"  % count + f)
                
            while True:
                try:
                    ans_file_ = int(input("\t\t Select Station: "))
                except:
                    print("\t\t Wrong selection")
                    continue
                if ans_file_ > count:
                    print ("\t\t Wrong selection.")
                    continue
                
                selected_file = selected_year + select_files[ans_file_]
                open_file(selected_file,historical_pats[ans_file],select_year[ans_year],select_files[ans_file_])
                break
            break
        break
    
def open_file(select_file,station,year,file):
    
    met_header = load_config()[0]['MET_INPUT']
    solar_header = load_config()[0]['SOLAR_INPUT']
    
    ##OUTPUT HEADER
    met_out_header = load_config()[0]['MET_HEADER']
    sol_out_header = load_config()[0]['SOLAR_HEADER']
    
    top_header('Main > Preprocessing > Translate Historical > '+str(station) +' > ' + str(year) + ' > ' +str(file))
    print('\t\tPlease select one file to translate  file: ')

    if 'MD' in  file:
        header_in = met_header
        header_out = met_out_header
    if 'SD' in file:
        header_in = solar_header
        header_out = sol_out_header
    if 'TD' in file:
        header_in = None
        header_out = None
    
    # READ FILE
    df = pd.read_csv(select_file, sep=",")
    
    ## SELECT ONLY COLUMNS INPUT
    df = df[header_in[1:]]
    ## IGNORE MULTINDEX INTO HISTORICAL DATA
    df = df.iloc[1:]
    ## Add column acronyms to the dataframe
    df['acronym'] = [station] * len(df)
    # Move acronym to the first column
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    # Remove column id if exists
    if 'id' in df.columns:
        df = df.drop('id', axis=1)

    print(df)

    # Set new header to the dataframe
    mux1 = pd.MultiIndex.from_tuples(header_out)
    # Get level 0 of the multiindex
    level_0 = mux1.get_level_values(0)
    # Fit df based on level 0
    for c in range(len(level_0)):
        print(df.columns[c], level_0[c])

    output = load_config()[0]['FORMATED_OUT']+str(station)+'/'+str(year)
    file = file.replace('.dat','.csv')
    if not os.path.exists(output):
        os.makedirs(output)

    df.to_csv(output+'/'+file, index=False)
