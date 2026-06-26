import pandas as pd
import happybase
import struct

# ============================================================
# CONNECT
# ============================================================
connection = happybase.Connection('127.0.0.1', port=9090)
connection.open()

# ============================================================
# CREATE TABLES
# ============================================================
# HBase doesn't have keyspaces, but we namespace with table name prefix: 'movies_'

# movies_by_genre
# Row key: genre + inverted_vote_average + id
# (inverted float for DESC order: store as 9.99... - vote_average)
# Column family: 'info'

existing = [t.decode() for t in connection.tables()]

if 'movies_by_genre' not in existing:
    connection.create_table(
        'movies_by_genre',
        {'info': dict(max_versions=1)}
    )
    print("Created table: movies_by_genre")

if 'movies_by_director' not in existing:
    connection.create_table(
        'movies_by_director',
        {'info': dict(max_versions=1)}
    )
    print("Created table: movies_by_director")

print("Tables ready.")

# ============================================================
# HELPERS
# ============================================================

def invert_float(value: float, max_val: float = 10.0) -> float:
    """Invert a float so that higher values sort first lexicographically."""
    return max_val - value

def make_row_key_genre(genre: str, vote_average: float, movie_id: int) -> bytes:
    """
    Row key for movies_by_genre:
      genre (padded) + '#' + inverted_vote_average (zero-padded 6 chars) + '#' + id (zero-padded 10 chars)
    This gives DESC order on vote_average, ASC on id — matching Cassandra clustering.
    """
    inverted = invert_float(vote_average)
    return f"{genre}#{inverted:010.6f}#{movie_id:010d}".encode()

def make_row_key_director(director: str, popularity: float, movie_id: int) -> bytes:
    """
    Row key for movies_by_director:
      director + '#' + inverted_popularity (zero-padded) + '#' + id (zero-padded)
    DESC order on popularity, ASC on id.
    """
    inverted = invert_float(popularity, max_val=100000.0)
    return f"{director}#{inverted:015.6f}#{movie_id:010d}".encode()

def to_bytes(value) -> bytes:
    """Convert any value to bytes for HBase storage."""
    return str(value).encode('utf-8')

# ============================================================
# LOAD DATA
# ============================================================
df = pd.read_csv("movies.csv", low_memory=False).fillna("")

def get_first_genre(genres_str):
    try:
        genres = eval(genres_str)
        return genres[0] if genres else "Unknown"
    except:
        return "Unknown"

df["first_genre"]   = df["genres_list"].apply(get_first_genre)
df["release_year"]  = pd.to_numeric(df["release_year"], errors="coerce").fillna(0).astype(int)
df["IMDB_Rating"]   = pd.to_numeric(df["IMDB_Rating"],  errors="coerce").fillna(0.0)

# ============================================================
# HELPERS — truncation
# ============================================================
MAX_TEXT = 1000  # max chars for large text fields

def safe_bytes(value, max_len=None) -> bytes:
    s = str(value)
    if max_len:
        s = s[:max_len]
    return s.encode('utf-8', errors='replace')

# ============================================================
# IMPORT — movies_by_genre (one row at a time, no batching)
# ============================================================
table_genre = connection.table('movies_by_genre')
print("Importing movies_by_genre...")

errors = 0
for i, row in enumerate(df.itertuples()):
    try:
        row_key = make_row_key_genre(row.first_genre, float(row.vote_average), int(row.id))

        table_genre.put(row_key, {
            b'info:genre':             safe_bytes(row.first_genre),
            b'info:release_year':      safe_bytes(row.release_year),
            b'info:id':                safe_bytes(row.id),
            b'info:title':             safe_bytes(row.title),
            b'info:vote_average':      safe_bytes(row.vote_average),
            b'info:vote_count':        safe_bytes(row.vote_count),
            b'info:popularity':        safe_bytes(row.popularity),
            b'info:director':          safe_bytes(row.Director),
            b'info:budget':            safe_bytes(row.budget),
            b'info:revenue':           safe_bytes(row.revenue),
            b'info:runtime':           safe_bytes(row.runtime),
            b'info:original_language': safe_bytes(row.original_language),
            b'info:status':            safe_bytes(row.status),
            b'info:overview':          safe_bytes(row.overview,   MAX_TEXT),
            b'info:genres_list':       safe_bytes(row.genres_list, MAX_TEXT),
            b'info:cast_list':         safe_bytes(row.Cast_list,   MAX_TEXT),
            b'info:imdb_rating':       safe_bytes(row.IMDB_Rating),
        })

        if i % 100 == 0:
            print(f"  {i} rows inserted (genre)...")

    except Exception as e:
        errors += 1
        print(f"  [SKIP] row {i} (id={row.id}): {e}")
        # reconnect after a broken socket
        try:
            connection.close()
        except:
            pass
        connection = happybase.Connection('127.0.0.1', port=9090)
        connection.open()
        table_genre = connection.table('movies_by_genre')

print(f"movies_by_genre — DONE ({errors} errors skipped)")

# ============================================================
# IMPORT — movies_by_director (one row at a time, no batching)
# ============================================================
table_director = connection.table('movies_by_director')
print("Importing movies_by_director...")

errors = 0
for i, row in enumerate(df.itertuples()):
    try:
        row_key = make_row_key_director(row.Director, float(row.popularity), int(row.id))

        table_director.put(row_key, {
            b'info:director':     safe_bytes(row.Director),
            b'info:release_year': safe_bytes(row.release_year),
            b'info:id':           safe_bytes(row.id),
            b'info:title':        safe_bytes(row.title),
            b'info:vote_average': safe_bytes(row.vote_average),
            b'info:popularity':   safe_bytes(row.popularity),
            b'info:genre':        safe_bytes(row.first_genre),
            b'info:budget':       safe_bytes(row.budget),
            b'info:revenue':      safe_bytes(row.revenue),
            b'info:genres_list':  safe_bytes(row.genres_list, MAX_TEXT),
            b'info:cast_list':    safe_bytes(row.Cast_list,   MAX_TEXT),
            b'info:imdb_rating':  safe_bytes(row.IMDB_Rating),
        })

        if i % 100 == 0:
            print(f"  {i} rows inserted (director)...")

    except Exception as e:
        errors += 1
        print(f"  [SKIP] row {i} (id={row.id}): {e}")
        try:
            connection.close()
        except:
            pass
        connection = happybase.Connection('127.0.0.1', port=9090)
        connection.open()
        table_director = connection.table('movies_by_director')

print(f"movies_by_director — DONE ({errors} errors skipped)")

connection.close()
print("DONE")