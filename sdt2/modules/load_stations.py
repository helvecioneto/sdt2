# -*- coding: utf-8 -*-
from modules.load_config import load_config
from modules.load_files import load_files
from os import listdir

config = load_config()
inputf = config[0]['INPUT']


def load_stations():
    file_names = [fn for fn in listdir(inputf) if not fn.startswith('.')]
    count = -1
    for f in file_names:
        count = count + 1
        print ("\t\t [%s]"  % count + f)
        
    while True:
        try:
            print()
            ans_file = int(input("\t\t Select Station: "))
        except:
            print("\t\t Wrong selection")
            continue
        if ans_file > count:
            print ("\t\t Wrong selection.")
            continue
    
        station = inputf + file_names[ans_file]
        files = load_files(station)
        break
    
    return files