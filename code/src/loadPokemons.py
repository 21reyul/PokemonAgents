import pandas as pd
import redis
import subprocess
import json

REMOVED_COLUMNS = ['Mean', 'Standard Deviation', 'Experience type', 'Experience to level 100', 'Final Evolution', 'Legendary', 'Alolan Form', 'Galarian Form', 'Against Normal', 'Against Fire', 'Against Water', 'Against Electric', 'Against Grass', 'Against Ice', 'Against Fighting', 'Against Poison', 'Against Ground', 'Against Flying', 'Against Psychic', 'Against Bug', 'Against Rock', 'Against Ghost', 'Against Dragon', 'Against Dark', 'Against Steel', 'Against Fairy', 'Height', 'Weight', 'BMI']
pokemon_sadd = "pokemon_sadd"
r = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)

# Function that reads the whole csv and then inserts all the information into a redis db
def pokemon_loading(filename = "Pokemon.csv"):
    pokemon_csv = pd.read_csv(filename) 
    pokemon_csv = pokemon_csv.drop(columns=REMOVED_COLUMNS)
    for i in range(int(subprocess.run(f"wc -l < {filename}", shell=True, capture_output=True, text=True).stdout.strip())):
        pokemon = pokemon_csv.loc[i]
        print(pokemon)
        data = {
            "name": str(pokemon["Name"]),
            "first_type": str(pokemon["Type 1"]),
            "second_type": str(pokemon["Type 2"]) if pd.notna(pokemon["Type 2"]) else None,
            "abilities": pokemon["Abilities"],  # if it's already a list or string
            "health": int(pokemon["HP"]),
            "attack": int(pokemon["Att"]),
            "defense": int(pokemon["Def"]),
            "sp_attack": int(pokemon["Spa"]),
            "speed": int(pokemon["Spe"]),
            "mega_evolution": True if pokemon["Mega Evolution"] == 1.0 else False
        }
        r.sadd(pokemon_sadd, json.dumps(data))

pokemon_loading()