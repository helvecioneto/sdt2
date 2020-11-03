# -*- coding: utf-8 -*-
from modules.top_header import top_header
from modules.load_config import load_config

from dependecies import *

def historic_generate():
    
    operation_dir = load_config()[0]['OPERATIONAL_IN']
    
    ## SET DEBUG DIR
    logging.basicConfig(filename=load_config()[0]['DEBUG_DIR']+'historical_debug.txt', filemode='a', format='\nProcess Date %(asctime)s \n %(message)s\n', datefmt='%d-%b-%y %H:%M:%S',level=os.environ.get("LOGLEVEL", "INFO"))
    
    
    top_header('Main > Preprocessing > Generate Historical')
    print('\t\tPlease select one stations to generate historical data: ')
    
    operational_stations = [fn for fn in listdir(operation_dir) if not fn.startswith('.')]
    if len(operational_stations) == 0:
        print('There is no data to be formatted')
        input('Press Enter to return')
        # pre_processing_menu()
        # return None
    
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
        
        ## MULTIINDEX
        mux = []
        for i in range(len(head1)):
            mux.append([str(head1[i]).lower(),str(head2[i]).lower()])
        mux = pd.MultiIndex.from_tuples(mux)
        
        ##REINDX TO DETECT
        try:
            locked_df_chk = locked_df_chk.set_index(0).asfreq(freqc)
            
            if len(locked_df_chk) == len(final_df):
                print('Aquivos iguais')
                print(final_df)
            else:
                print('Arquivo reprovado')
                print('Total de linhas vazias :',locked_df_chk[1].isna().sum())
                emptyrows = locked_df_chk[1].index[locked_df_chk[1].apply(np.isnan)]
                # print('Linhas vazias: ',emptyrows)
                
               
                
                ## SAVE PROCESS
                output_dir = load_config()[0]['HISTORICAL_OUT'] + str(operational_stations[ans_file]).upper() + '/' + str(selected_year) + '/'
                output_file_name = str(operational_stations[ans_file]).upper()  + '_' + str(selected_year) + '_' + str(locked_df_chk.index[0].strftime('%j')) + '_a_' + str(locked_df_chk.index[-1].strftime('%j')) + '_' +  str(dataTypes[ans_type]) + '.dat'
                
                ## CREATE DIR
                pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
                ## CREATE VERSION DIRS
                pathlib.Path(output_dir+'/versions').mkdir(parents=True, exist_ok=True)
                
                 ## ADD COLUMNS
                locked_df_chk = locked_df_chk.reset_index()
                locked_df_chk.columns = mux

                file__ = output_dir + output_file_name
                # ## CHEACK IF FILES EXISTS
                if os.path.isfile(file__):
                    warningmsg = ('\nSTATION-> '+str(output_file_name[:-4]) + '  \nN. NULL ROWS:-> '+str(len(emptyrows))+'\nNULL ROWS:\n'+str(emptyrows) +'\n')
                    logging.warning(warningmsg)
                    print(locked_df_chk)
                    
                    ## CHECK LAST VERSION
                    if len(os.listdir(output_dir+'/versions') ) == 0:
                        shutil.move(file__, output_dir+'/versions/'+output_file_name+'.v01')
                        ## CREATE FILE
                        locked_df_chk.to_csv(file__,index=False)
                        
                        warningmsg = ('\nSTATION-> '+str(output_file_name[:-4]) + ' File version(0)  \nN. NULL ROWS:-> '+str(len(emptyrows)) +'\n')
                        logging.warning(warningmsg)

                    else:    
                        versions = [fn for fn in listdir(output_dir+'/versions/') if not fn.startswith('.')]
                        shutil.move(file__, output_dir+'/versions/'+output_file_name+'v0'+str((int(versions[-1][-2:]) + 1)))
                        ## CREATE FILE
                        locked_df_chk.to_csv(file__,index=False)
                        
                        warningmsg = ('\nSTATION-> '+str(output_file_name[:-4]) + ' File version('+ str(int(versions[-1][-2:]) + 1)+ ')  \nN. NULL ROWS:-> '+str(len(emptyrows)) +'\n')
                        logging.warning(warningmsg)

                else:
                    warningmsg = ('\nSTATION-> '+str(output_file_name[:-4]) + ' File version(0)  \nN. NULL ROWS:-> '+str(len(emptyrows)) +'\n')
                    logging.warning(warningmsg)
                    ## CREATE FILE
                    print(locked_df_chk)
                    locked_df_chk.to_csv(file__,index=False)

                
        except:
            print('Arquivo reprovado')
            locked_df_chk = locked_df_chk.set_index(0)
            duplicated_lines = locked_df_chk[locked_df_chk.index.duplicated()]
            print('Linhas duplicadas : ',duplicated_lines)
            warningmsg = ('\nSTATION-> '+str(output_file_name[:-4]) + '  \nN. DUPLICATED ROWS:-> '+str(len(duplicated_lines))+'\DUPLICATED ROWS:\n'+str(duplicated_lines) +'\n')
            logging.warning(warningmsg)
            
       