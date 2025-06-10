import pandas as pd
import subprocess
import json

REMOVED_COLUMNS = ['Mean', 'Standard Deviation', 'Experience type', 'Experience to level 100', 'Final Evolution', 'Legendary', 'Alolan Form', 'Galarian Form', 'Against Normal', 'Against Fire', 'Against Water', 'Against Electric', 'Against Grass', 'Against Ice', 'Against Fighting', 'Against Poison', 'Against Ground', 'Against Flying', 'Against Psychic', 'Against Bug', 'Against Rock', 'Against Ghost', 'Against Dragon', 'Against Dark', 'Against Steel', 'Against Fairy', 'Height', 'Weight', 'BMI']

# Function that reads the whole csv and then inserts all the information into a redis db
def pokemon_loading(filename = "Pokemon.csv"):
    pokemon_csv = pd.read_csv(filename) 
    pokemon_csv = pokemon_csv.drop(columns=REMOVED_COLUMNS)
    for i in range(int(subprocess.run(f"wc -l < {filename}", shell=True, capture_output=True, text=True).stdout.strip())):
        pokemon = pokemon_csv.loc[i]
        data = {
                "name": str(pokemon["Name"]),
                "first_type": str(pokemon["Type 1"]),
                "second_type": str(pokemon["Type 2"]) if pd.notna(pokemon["Type 2"]) else "",
                "abilities": pokemon["Abilities"], 
                "health": int(pokemon["HP"]),
                "attack": int(pokemon["Att"]),
                "defense": int(pokemon["Def"]),
                "sp_attack": int(pokemon["Spa"]),
                "speed": int(pokemon["Spe"]),
                "mega_evolution": 1 if pokemon["Mega Evolution"] == 1.0 else 0
            }
            
        if (subprocess.run(["mongosh", "--eval", f"db.pokemon.findOne({data})"], capture_output=True, text=True).stdout.strip() == "null"):
            print(subprocess.run(["mongosh", "--eval", f"db.pokemon.insertOne({data})"]))
            #print(subprocess.run(["mongosh", "--eval", f"db.pokemon.deleteOne({data})"]))
        else:
            print(f"{pokemon["Name"]} already stored")
        break
    #print(subprocess.run(["mongosh", "--eval", f"db.pokemon.findOne({data})"]))

pokemon_loading()
print(subprocess.run(["mongosh", "--eval", "db.pokemon.find().count()"]))