import pandas as pd
import json

df = pd.read_csv('movies.csv', low_memory=False)
ids = df['id'].dropna().astype(int).tolist()[:50000]

with open('shared_ids.json', 'w') as f:
    json.dump(ids, f)

print(f"Зачувани {len(ids)} ID-а во shared_ids.json")