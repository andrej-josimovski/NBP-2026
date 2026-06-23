import pandas as pd

df = pd.read_csv("movies.csv")

print("Columns:")
print(df.columns.tolist())

print("\nFirst rows:")
print(df.head())

print("\nInfo:")
print(df.info())

