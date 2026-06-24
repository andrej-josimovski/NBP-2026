import pandas as pd
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection

# Поврзување
cluster = Cluster(['127.0.0.1'], connection_class=AsyncioConnection)
session = cluster.connect('movies')

# ============================================================
# МОДЕЛ 1 - по жанр (genre-centric)
# ============================================================
session.execute("DROP TABLE IF EXISTS movies_by_genre")
session.execute("""
CREATE TABLE IF NOT EXISTS movies_by_genre (
    genre TEXT,
    release_year INT,
    id INT,
    title TEXT,
    vote_average FLOAT,
    vote_count INT,
    popularity FLOAT,
    director TEXT,
    budget BIGINT,
    revenue BIGINT,
    runtime INT,
    original_language TEXT,
    status TEXT,
    overview TEXT,
    genres_list TEXT,
    cast_list TEXT,
    imdb_rating FLOAT,
    PRIMARY KEY (genre, vote_average, id)
) WITH CLUSTERING ORDER BY (vote_average DESC, id ASC)
""")

# ============================================================
# МОДЕЛ 2 - по режисер (director-centric)
# ============================================================
session.execute("DROP TABLE IF EXISTS movies_by_director")
session.execute("""
CREATE TABLE IF NOT EXISTS movies_by_director (
    director TEXT,
    release_year INT,
    id INT,
    title TEXT,
    vote_average FLOAT,
    popularity FLOAT,
    genre TEXT,
    budget BIGINT,
    revenue BIGINT,
    genres_list TEXT,
    cast_list TEXT,
    imdb_rating FLOAT,
    PRIMARY KEY (director, popularity, id)
) WITH CLUSTERING ORDER BY (popularity DESC, id ASC)
""")

print("Табелите се креирани.")

# ============================================================
# ИМПОРТ
# ============================================================
df = pd.read_csv('D:/Proekt/NBP/movies.csv', low_memory=False)
df = df.fillna('')

def get_first_genre(genres_str):
    try:
        genres = eval(genres_str)
        return genres[0] if genres else 'Unknown'
    except:
        return 'Unknown'

df['first_genre'] = df['genres_list'].apply(get_first_genre)
df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce').fillna(0).astype(int)
df['IMDB_Rating'] = pd.to_numeric(df['IMDB_Rating'], errors='coerce').fillna(0.0)

# Импорт во модел 1
print("Импортирање во movies_by_genre...")
for i, row in df.iterrows():
    session.execute("""
        INSERT INTO movies_by_genre 
        (genre, release_year, id, title, vote_average, vote_count, popularity, director, budget, revenue, runtime, original_language, status, overview, genres_list, cast_list, imdb_rating)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['first_genre'], row['release_year'], int(row['id']),
        row['title'], float(row['vote_average']), int(row['vote_count']),
        float(row['popularity']), row['Director'],
        int(row['budget']), int(row['revenue']), int(row['runtime']),
        row['original_language'], row['status'], row['overview'],
        row['genres_list'], row['Cast_list'], float(row['IMDB_Rating'])
    ))
    if i % 10000 == 0:
        print(f"  {i} редови импортирани...")

# Импорт во модел 2
print("Импортирање во movies_by_director...")
for i, row in df.iterrows():
    session.execute("""
        INSERT INTO movies_by_director
        (director, release_year, id, title, vote_average, popularity, genre, budget, revenue, genres_list, cast_list, imdb_rating)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Director'], row['release_year'], int(row['id']),
        row['title'], float(row['vote_average']), float(row['popularity']),
        row['first_genre'], int(row['budget']), int(row['revenue']),
        row['genres_list'], row['Cast_list'], float(row['IMDB_Rating'])
    ))
    if i % 10000 == 0:
        print(f"  {i} редови импортирани...")

print("Импортот е завршен!")
cluster.shutdown()