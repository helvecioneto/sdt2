# -*- coding: utf-8 -*-
#import sdt2
import sys
from modules.top_header import top_header
from modules.split_data import split_data
from modules.load_stations import load_stations_02
from modules.automatic import load_data_type
from modules.format_new_data import format_new_data

def main_menu():
    top_header('Main Menu')
    print('\t\tPlease select an option: ')
    choice = input("""
                  0: Format New Data
                  1: Anemometric Data
                  2: Solar Data
                  3: Sky Camera Data
                  4: Automatic detection
                  Q: Quit
                  Please enter your choice: """)

    if choice == "Format New Data" or choice =="0":
        format_new_data()
    elif choice == "Anemometric Data" or choice =="1":
        anemomectric()
    elif choice == "Solar Data" or choice =="2":
        solarimetric()
    elif choice == "Sky Camera Data" or choice =="3":
        skycamera()
    elif choice == "Automatic detection" or choice =="4":
        automatic()
    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select one option")
        print("Please try again")
        main_menu()

    
def anemomectric():
    print('Anemometric')

def solarimetric():
    top_header('Main Menu > Solarimetric')
    print('\t\tPlease select an option')
    choice = input("""
                  1: Split (Ambiental/Solar)
                  2: Create Headers Log
                  Q: Quit
                  Please enter your choice: """)

    if choice == "Split (Ambiental/Solar)" or choice =="1":
        split_data()
    elif choice == "Create Headers Log" or choice =="2":
        print('Função não implementada')
        main_menu()
    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select either 1 or 2")
        print("Please try again")
        main_menu()
        
def skycamera():
    top_header('Main Menu > Sky Camera')
    print('\t\tPlease select an option')
    load_stations_02()
    
def automatic():
    top_header('Main Menu > Detect and format ')
    print('\t\tPlease select an option')
    load_data_type()
    
