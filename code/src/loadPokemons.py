import pandas as pd
import redis
import subprocess

REMOVED_COLUMNS = ['Mean', 'Standard Deviation', 'Experience type', 'Experience to level 100', 'Final Evolution', 'Legendary', 'Alolan Form', 'Galarian Form', 'Against Normal', 'Against Fire', 'Against Water', 'Against Electric', 'Against Grass', 'Against Ice', 'Against Fighting', 'Against Poison', 'Against Ground', 'Against Flying', 'Against Psychic', 'Against Bug', 'Against Rock', 'Against Ghost', 'Against Dragon', 'Against Dark', 'Against Steel', 'Against Fairy', 'Height', 'Weight', 'BMI']
pokemon_sadd = "pokemon_sadd"
client = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)

# Function that reads the whole csv and then inserts all the information into a redis db
def pokemon_loading(filename = "Pokemon.csv"):
    pokemon_csv = pd.read_csv(filename)
    pokemon_csv = pokemon_csv.drop(columns=REMOVED_COLUMNS)
    print(pokemon_csv)
    # subprocess.run(f"wget -qO - {url} | tar -xvz -C {DATA_DIR}", shell=True, check=True)
    for pokemon in range(subprocess.run(f"ls -la {filename} | wc -l", shell=True, capture_output=True, text=True)):
        break
    
pokemon_loading()