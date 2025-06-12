import subprocess

subprocess.run(["mongosh", "--eval", "db.pokemon.deleteMany({})"])

