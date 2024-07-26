import toml
import json

with open("concentration.csv") as source:
    response = []
    for line in source:
        words = line.split(",")
        # print(words)
        idd = words[0].split("/")[-1][:-1]
        unit = words[3]

        print(f'"{idd}" = {unit}')

# with open("data.toml", "w") as target:
#     for entry in response:
#         number = entry["id"].split("/")[-1]
#         target.write(f'{number} = "{entry["notation"]}" \n')
