import os
import csv
import sqlite3
from datetime import datetime
from typing import final


def create_database():
    conn = sqlite3.connect('gruppoLuci\weight_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS weight_data
                 (id INTEGER PRIMARY KEY,
                 filter_code TEXT,
                 initial_weight REAL,
                 final_weight REAL,
                 gross_weight REAL,
                 unit TEXT,
                 timestamp TEXT)''')
    conn.commit()
    conn.close()

def insert_data_to_database(filter_code, weight, unit, exists):
    conn = sqlite3.connect('gruppoLuci\weight_data.db')
    c = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if exists:
        # Aggiorna il peso finale per il codice filtro esistente
        c.execute("UPDATE weight_data SET final_weight = ?, unit = ?, timestamp = ? WHERE filter_code = ?",
                  (weight, unit, timestamp, filter_code))
    else:
        # Inserisci un nuovo record nel database per il codice filtro
        c.execute("INSERT INTO weight_data (filter_code, initial_weight, unit, timestamp) VALUES (?, ?, ?, ?)",
                  (filter_code, weight, unit, timestamp))
        
    conn.commit()
    conn.close()

def insert_gross_weight(filter_code, gross_weight):
    conn = sqlite3.connect('gruppoLuci\weight_data.db')
    c = conn.cursor()
    c.execute("UPDATE weight_data SET gross_weight = ? WHERE filter_code = ?",
                  (gross_weight, filter_code))
    
    conn.commit()
    conn.close()


def filter_code_exists_in_database(filter_code):
    conn = sqlite3.connect('gruppoLuci\weight_data.db')
    c = conn.cursor()
    c.execute("SELECT filter_code FROM weight_data WHERE filter_code = ?", (filter_code,))
    result = c.fetchone()
    conn.close()
    return result is not None

def get_filter_code_weight(filter_code):
    conn = sqlite3.connect('gruppoLuci\weight_data.db')
    c = conn.cursor()
    c.execute("SELECT initial_weight FROM weight_data WHERE filter_code = ?", (filter_code,))
    result = c.fetchone()
    conn.close()
    
    if result:  # Se c'è un risultato
        return result[0]  # Ritorna il primo elemento della tupla (il peso)
    else:
        return None  # Se non c'è nessun risultato, ritorna None
    
def process_weight_data(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        weight_data = list(reader)
        
        if not weight_data:
            return
        print(weight_data)
        weights_dict = {}  # Dizionario per tenere traccia dei pesi per ogni codice
        unit = None
        
        for row in weight_data:
            if row[0] == 'EMS':
                # Gestisci il caso in cui viene letto un nuovo codice di filtro
                filter_code = row[1]
                
                if '-' in filter_code:
                    current_filter_code = filter_code[:-2]
                else:
                
                        
                    current_filter_code = filter_code
                    unit = row[2]  # unità di misura
                
                if current_filter_code not in weights_dict:
                    weights_dict[current_filter_code] = []  # Inizializza una lista per il nuovo codice
                    
            elif row[0] == 'G':
                # Gestisci il caso in cui viene letto un peso
                current_weights = weights_dict[current_filter_code]
                current_weights.append(float(row[1]))
                unit = row[2]  # unità di misura
                
                # Inserisci i dati nel dizionario ogni volta che viene letto un nuovo peso
                weights_dict[current_filter_code] = current_weights
        
        # Calcola la media dei pesi per ogni codice e inseriscili nel database
        for filter_code, weights in weights_dict.items():
            
            if(filter_code_exists_in_database(filter_code)):
                
                        average_weight = round(sum(weights) / len(weights),3)
                        w = get_filter_code_weight(filter_code)
                        gross_weight = average_weight - w
                        insert_data_to_database(filter_code, average_weight, unit, True)
                        insert_gross_weight(filter_code, gross_weight)
                        
            elif(sum(weights)!= 0):           
                average_weight = round(sum(weights) / len(weights),3)
                insert_data_to_database(filter_code, average_weight, unit, False)

            

                

# Usage example
filename = 'gruppoLuci\CUBIS_0043804204_2024-01-16_10-07-09_1.csv'
archive_folder = 'archive'

create_database()
process_weight_data(filename)
