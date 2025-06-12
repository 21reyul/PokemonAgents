import subprocess

# subprocess.run(["mongosh", "--eval", "db.pokemon.deleteMany({})"])
# subprocess.run(["mongosh", "--eval", "db.movements.deleteMany({})"])
subprocess.run(["mongosh", "--eval", "db.movementsPokemon.deleteMany({})"])
# subprocess.run(["mongosh", "--eval", "db.pokemon.dropIndexes()"])
# subprocess.run(["mongosh", "--eval", "db.movements.dropIndexes()"])
subprocess.run(["mongosh", "--eval", "db.movementsPokemon.dropIndexes()"])