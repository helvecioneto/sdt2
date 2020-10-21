# -*- coding: utf-8 -*-
#import sdt2
import sys
from modules.top_header import top_header
from modules.split_data import split_data
from modules.load_stations import load_stations_02
from modules.download_data import download_stations

def main_menu():
    top_header('Main Menu')
    print('\t\tPlease select operational mode: ')
    choice = input("""
                  0: Preprocessing Mode
                  1: Translate Mode
                  Q: Quit
                  Please enter your choice: """)
    
    if choice == "0: Preprocessing Mode" or choice =="0":
        pre_processing_menu()
    elif choice == "1: Translate Mode" or choice =="1":
        translate_menu()
    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select one option")
        print("Please try again")
        main_menu()   
                  

def pre_processing_menu():
    top_header('Main Menu > Preprocessing Mode')
    print('\t\tPlease select operational mode: ')
    choice = input("""
                  0: Download Data
                  1: Generate Historical Data
                  2: Translate Historical Data
                  B: Back
                  Q: Quit
                  Please enter your choice: """)
                  
    if choice == "Download Data" or choice =="0":
        download_stations()
    elif choice == "Generate Historical Data" or choice =="1":
        print('Gerar historicos')
    elif choice == "Translate Historical Data" or choice =="2":
        print('Traduzir historicos')
    elif choice == "B" or choice =="b":
        main_menu()
    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select one option")
        print("Please try again")
        pre_processing_menu()
    
def translate_menu():
    top_header('Main Menu > Translate Mode')
    print('\t\tPlease select operational mode: ')
    choice = input("""
                  0: Translate Anemometric Data
                  1: Translate and Split Solar Data
                  2: Translate Sky Camera Data
                  B: Back
                  Q: Quit
                  Please enter your choice: """)

    if choice == "Translate Anemometric Data" or choice =="0":
        anemomectric()
    elif choice == "Translate and Split Solar Data" or choice =="1":
        split_data()
    elif choice == "Translate Sky Camera Data" or choice =="2":
        skycamera()
    elif choice == "B" or choice =="b":
        main_menu()
    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select one option")
        print("Please try again")
        translate_menu()

def anemomectric():
    print('Anemometric')

def skycamera():
    top_header('Main Menu > Sky Camera')
    print('\t\tPlease select an option')
    load_stations_02()
