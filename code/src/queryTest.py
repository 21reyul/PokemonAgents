import subprocess

# subprocess.run(["mongosh", "--eval", 'db.movements.findOne({"id": {$gt: 825}})'])
# subprocess.run(["mongosh", "--eval", 'db.movements.find({"identifier": {$eq: "eerie-spell"}})'])
# subprocess.run(["mongosh", "--eval", 'db.movements.find({}).count()'])
# subprocess.run(["mongosh", "--eval", 'db.movementsPokemon.findOne({"pokemon_id": {$eq: 1}})'])
# subprocess.run(["mongosh", "--eval", 'db.movements.findOne({"id": {$eq: 36}})'])
subprocess.run(["mongosh", "--eval", 'db.pokemon.find({}).count()'])
subprocess.run(["mongosh", "--eval", 'db.movements.find({}).count()'])
subprocess.run(["mongosh", "--eval", 'db.movementsPokemon.find({}).count()'])