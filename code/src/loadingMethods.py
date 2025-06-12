import pandas as pd
import subprocess

TYPE_DICTIONARY = {}
REMOVED_POKEMON_COLUMNS = ['Mean', 'Standard Deviation', 'Experience type', 'Experience to level 100', 'Final Evolution', 'Legendary', 'Alolan Form', 'Galarian Form', 'Against Normal', 'Against Fire', 'Against Water', 'Against Electric', 'Against Grass', 'Against Ice', 'Against Fighting', 'Against Poison', 'Against Ground', 'Against Flying', 'Against Psychic', 'Against Bug', 'Against Rock', 'Against Ghost', 'Against Dragon', 'Against Dark', 'Against Steel', 'Against Fairy', 'Height', 'Weight', 'BMI']
REMOVED_MOVES_COLUMNS = ['contest_type_id', 'contest_effect_id', 'super_contest_effect_id']

# Function that stores on a local dictionary the pokemon types
def types_loading(filename = "data/csv/Types.csv"):
    types_csv = pd.read_csv(filename)
    for i in range(len(filename)):
        types = types_csv.loc[i]
        TYPE_DICTIONARY[str(types["identifier"])] = int(types["id"])

# Function that reads the whole csv and stores it
def pokemon_loading(filename = "data/csv/Pokemon.csv"):
    pokemon_csv = pd.read_csv(filename) 
    pokemon_csv = pokemon_csv.drop(columns=REMOVED_POKEMON_COLUMNS)
    print(pokemon_csv.columns.tolist())
    for i in range(len(filename)):
        pokemon = pokemon_csv.loc[i]
        data = {
                "id": int(pokemon["Number"]),
                "name": str(pokemon["Name"]),
                "first_type": TYPE_DICTIONARY[(str(pokemon["Type 1"])).lower()],
                "second_type": TYPE_DICTIONARY[(str(pokemon["Type 2"])).lower()] if str(pokemon["Type 2"]) else "",
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
        else:
            print(f"{pokemon["Name"]} already stored")
        break

# Function that reads MovesStorage.csv and stores the information needed1
def moves_loading(filename = "data/csv/MovesStorage.csv"):
    moves_csv = pd.read_csv(filename)
    moves_csv = moves_csv.drop(columns=REMOVED_MOVES_COLUMNS)
    print(moves_csv.columns.tolist())
    for i in range(len(filename)):
        data = {
            
        }

if __name__ == "__main__":
    types_loading()
    #pokemon_loading()
    moves_loading()
    #print(subprocess.run(["mongosh", "--eval", "db.pokemon.find().count()"]))