import subprocess

subprocess.run(["mongosh", "--eval", 'db.pokemon.findOne({name: "Charmander"})'])