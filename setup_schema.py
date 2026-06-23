from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS movies
WITH replication = {
    'class':'SimpleStrategy',
    'replication_factor':1
}
""")

session.set_keyspace('movies')

print("Using keyspace movies")
print("Setup completed successfully.")

cluster.shutdown()