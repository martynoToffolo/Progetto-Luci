import pandas as pd
import csv

with open('gruppoLuci\CUBIS_0043804204_2024-01-16_10-07-09.csv', 'rb') as csvfile:

    content = csvfile.read().decode('UTF-16LE')
    
    # Remove the BOM if present
    if content.startswith('\ufeff'):
        content = content[1:]

    lines = content.splitlines()

    reader = csv.reader(lines, delimiter='\t')
    list = list(reader)
    list.pop()
    weight_data = list
    
    print(weight_data)
