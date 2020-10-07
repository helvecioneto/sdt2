# -*- coding: utf-8 -*-
from ftplib import FTP
import os # allows me to use os.chdir
from modules.top_header import top_header
import sys
import time

port=21
ip=None
password=None
user=None

def format_data():
    print('Formatar')
    
def connection():
    
    global user,password,ip
    
    top_header(('Main Menu > Format Data > Connect into FTP'))
    print('\t\tFTP Config:')
    print('\t\tIP: ',ip)
    print('\t\tUser:',user)
    print('\t\tPassword:',password)
    print('\t\tPlease select an option: ')
    
    choice = input("""
                  0: Connect
                  1: Set IP
                  2: Set User
                  3: Set Password
                  Q: Quit
                  Please enter your choice: """)


    if choice == "Connect" or choice =="0":
       
        print('Connecting....')
        try:
            ftp = FTP(ip)
            ftp.login(user,password)
            time.sleep(2)
            print('Connection established!')
            list_stations(ftp)
        except:
            time.sleep(2)
            print('Connection failed')
            print('Please try again!')
            input()
            connection()


    elif choice == "Set IP" or choice =="1":
        ip = input("\t\tIP:  ")
        connection()
        
    elif choice == "Set User" or choice =="2":
        user = input("\t\tIP:  ")
        connection()
        
    elif choice == "Set Password:" or choice =="3":
        password = input("\t\tPassword:  ")
        connection()
        
    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select one option")
        print("Please try again")
        connection()
        
        
def list_stations(ftp):
    
    top_header(('Main Menu > Format Data > Connect into FTP > Select station'))
    print('\t\tPlease select an option: \n')
    
    ## FTP Dir
    directory ="/labgama/redesonda/"
    
    ## Add STATIONS
    lt_stations = []
    direc_st = []
    for station in ftp.nlst(directory):
        if "." in station:
            pass
        else:
            lt_stations.append(station[-3:])
            direc_st.append(station)
         
    lt_stations = [x.upper() for x in lt_stations]
    
    count = -1
    for f in lt_stations:
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
        
        print('Estacao selectionada',direc_st[ans_file])
        break