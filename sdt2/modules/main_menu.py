# -*- coding: utf-8 -*-
#import sdt2
import sys
from modules.top_header import top_header
from modules.split_data import split_data

def main_menu():
    top_header('Main Menu')
    print('\t\tPlease select one option')
    choice = input("""
                  1: Anemometric Data
                  2: Solar Data
                  Q: Quit
                  Please enter your choice: """)

    if choice == "Anemometric Data" or choice =="1":
        anemomectric()
    elif choice == "Solar Data" or choice =="2":
        solarimetric()
    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select either 1 or 2")
        print("Please try again")
        main_menu()
    


    
def anemomectric():
    print('Anemometric')

def solarimetric():
    top_header('Main Menu > Solarimetric')
    print('\t\tPlease select one option')
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
    
