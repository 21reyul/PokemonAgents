import pandas as pd
import json
import subprocess
import multiprocessing
from tqdm import tqdm

TYPE_DICTIONARY = {}
REMOVED_POKEMON_COLUMNS = ['Mean', 'Standard Deviation', 'Experience type', 'Experience to level 100', 'Final Evolution', 'Legendary', 'Alolan Form', 'Galarian Form', 'Against Normal', 'Against Fire', 'Against Water', 'Against Electric', 'Against Grass', 'Against Ice', 'Against Fighting', 'Against Poison', 'Against Ground', 'Against Flying', 'Against Psychic', 'Against Bug', 'Against Rock', 'Against Ghost', 'Against Dragon', 'Against Dark', 'Against Steel', 'Against Fairy', 'Height', 'Weight', 'BMI']
REMOVED_MOVES_COLUMNS = ['contest_type_id', 'contest_effect_id', 'super_contest_effect_id']
REMOVED_MOVES_POKEMON_COLUMNS = ['version_group_id', 'pokemon_move_method_id', 'order']

# Auxiliar function that devides the df on some bulks to optimize the insertions 
def batch(iterable, n):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

# Function that gets the range of execution for the storage
def range_calculus(number_entries, process):
    ranges = {}
    lines_per_process = number_entries // process
    for p in range(process):
        start = p * lines_per_process
        if p == process - 1:
            end = start + lines_per_process
        else:
            end = start + lines_per_process - 1
        ranges[p] = {
            "start" : start,
            "end" : end
        }
    return ranges


# Function that stores on a local dictionary the pokemon types
def types_loading(filename = "data/csv/Types.csv"):
    types_csv = pd.read_csv(filename)
    for i in range(int(subprocess.run(f"wc -l < {filename}", shell=True, capture_output=True, text=True).stdout.strip())):
        types = types_csv.loc[i]
        TYPE_DICTIONARY[str(types["identifier"])] = int(types["id"])

# Function that reads the whole csv and stores it
def pokemon_loading(filename = "data/csv/Pokemon.csv"):
    documents = []
    pokemon_csv = pd.read_csv(filename) 
    pokemon_csv = pokemon_csv.drop(columns=REMOVED_POKEMON_COLUMNS)
    for _, pokemon in tqdm(pokemon_csv.iterrows(), total = len(pokemon_csv)):
        data = {
                "id": int(pokemon["Number"]),
                "name": str(pokemon["Name"]),
                "generation": int(pokemon["Generation"]),
                "first_type": TYPE_DICTIONARY[(str(pokemon["Type 1"])).lower()],
                "abilities": pokemon["Abilities"], 
                "health": int(pokemon["HP"]),
                "attack": int(pokemon["Att"]),
                "defense": int(pokemon["Def"]),
                "sp_attack": int(pokemon["Spa"]),
                "speed": int(pokemon["Spe"]),
            }

        if pd.notna(pokemon["Type 2"]):
            data["second_type"] = TYPE_DICTIONARY[str(pokemon["Type 2"]).lower()]

        if pd.notna(pokemon["Mega Evolution"]):
            data["mega_evolution"] = 1

        documents.append(data)

        #subprocess.run(["mongosh", "--eval", f"db.pokemon.createIndex({json.dumps({"id": data["id"], "generation": data["generation"]})}, {{unique: true}})"])

    for b in batch(documents, 100):
        print(subprocess.run(["mongosh", "--eval", f"db.pokemon.insertMany({b})"]))

# Function that reads MovesStorage.csv and stores the information needed1
def moves_loading(filename = "data/csv/MovesStorage.csv"):
    documents = []
    moves_csv = pd.read_csv(filename)
    moves_csv = moves_csv.drop(columns=REMOVED_MOVES_COLUMNS)
    print(moves_csv.columns.tolist())
    for _, moves in tqdm(moves_csv.iterrows(), total = len(moves_csv)):
        data = {
            "id": int(moves["id"]),
            "identifier": str(moves["identifier"]),
            "generation_id": int(moves["generation_id"]),
            "type_id": int(moves["type_id"]),
            "pp": int(moves["pp"]),
            "priority": int(moves["priority"]),
            "target_id": int(moves["target_id"]),
            "damage_class_id": int(moves["damage_class_id"]),
        }

        if pd.notna(moves["power"]):
            data["power"] = int(moves["power"])
        
        if pd.notna(moves["accuracy"]):
            data["accuracty"] = int(moves["accuracy"])

        if pd.notna(moves["effect_id"]):
            data["effect_id"] = int(moves["effect_id"])

        if pd.notna(moves["effect_chance"]):
            data["effect_chance"] = int(moves["effect_chance"])

        #subprocess.run(["mongosh", "--eval", f"db.movements.createIndex({data}, {{unique: true}})"])

        documents.append(data)

    for b in batch(documents, 100):
        print(subprocess.run(["mongosh", "--eval", f"db.movements.insertMany({b})"]))

# Auxiliar function that stores the data to the db
def loading_moves_pokemon(moves_pokemon_csv):
    documents = []
    for _, moves_pokemon in tqdm(moves_pokemon_csv.iterrows(), total = len(moves_pokemon_csv)):
        data = {
            "pokemon_id": int(moves_pokemon["pokemon_id"]),
            "move_id": int(moves_pokemon["move_id"]),
            "level": int(moves_pokemon["level"])
        }
        #subprocess.run(["mongosh", "--eval", f"db.movementsPokemon.createIndex({data}, {unique: true})"])
        
        #if(subprocess.run(["mongosh", "--eval", f"db.movementsPokemon.findOne({data})"], capture_output=True, text=True).stdout.strip() == "null"):
        documents.append(data)
        
        # else:
        #     print(f"{data} is already stored")
    for b in batch(documents, 1000):
        subprocess.run(["mongosh", "--eval", f"db.movementsPokemon.insertMany({b})"])
    

# This function stores the relation between moves and pokemons
def relation_moves_pokemon(filename = "data/csv/PokemonMoves.csv", process = 4):
    process_array = []
    moves_pokemon_csv = pd.read_csv(filename)
    moves_pokemon_csv = moves_pokemon_csv.drop(columns = REMOVED_MOVES_POKEMON_COLUMNS)
    moves_pokemon_csv = moves_pokemon_csv.drop_duplicates()
    ranges = range_calculus(len(moves_pokemon_csv), process)
    for p in range(process):
        partial_df = moves_pokemon_csv.iloc[ranges[p]["start"]:ranges[p]["end"]]
        print(f"Start: {ranges[p]["start"]}\tEnd: {ranges[p]["end"]}\tProcessed: {len(partial_df)}")
        proc = multiprocessing.Process(target=loading_moves_pokemon, args=(partial_df,))
        proc.start()
        process_array.append(proc)

    print("Waiting for the end of all process...")
    for p in process_array:
        p.join()

if __name__ == "__main__":
    types_loading()
    #pokemon_loading()
    #moves_loading()
    relation_moves_pokemon()