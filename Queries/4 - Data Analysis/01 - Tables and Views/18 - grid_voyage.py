# Create the shortest path between the grids

import psycopg2
from psycopg2.extras import execute_values

# Database connection info
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "thesis_v5",
    "user": "postgres",
    "password": "Skat0malakas"
}

# Batch configuration
BATCH_SIZE = 10
DISTANCE_THRESHOLD = 11500
MAX_NEIGHBORS = 6

# Connect to PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True  # ensures each INSERT is persisted immediately
cur = conn.cursor()

# Get max ID
cur.execute("SELECT MAX(id) FROM context_data.grid_voyage;")
max_id = cur.fetchone()[0]

print(f"Max ID: {max_id}")

# Loop over batches
for start_id in range(237730, max_id + 1, BATCH_SIZE):
    end_id = start_id + BATCH_SIZE - 1
    print(f"Processing batch {start_id}–{end_id}...")

    query = f"""
        INSERT INTO context_data.grid_voyage_network (
            source_grid_id,
            target_grid_id,
            track,
            cost
        )
        SELECT
            v1.id AS source_grid_id,
            v2.id AS target_grid_id,
            ST_MakeLine(
                ST_Transform(v1.centr_3857, 4326),
                ST_Transform(v2.centr_3857, 4326)
            ) AS track,
            ST_Distance(v1.centr_3857, v2.centr_3857) AS cost
        FROM context_data.grid_voyage v1
        JOIN LATERAL (
            SELECT id, centr_3857
            FROM context_data.grid_voyage v2
            WHERE v1.id < v2.id
              AND ST_DWithin(v1.centr_3857, v2.centr_3857, {DISTANCE_THRESHOLD})
            ORDER BY v1.centr_3857 <-> v2.centr_3857
            LIMIT {MAX_NEIGHBORS}
        ) v2 ON TRUE
        WHERE v1.id BETWEEN {start_id} AND {end_id};
    """


    try:
        cur.execute(query)
        print(f"✅ Batch {start_id}–{end_id} committed.")
    except Exception as e:
        print(f"❌ Error in batch {start_id}–{end_id}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("🎉 Done!")


print("*** Creating Table Indexes ***")

sql_statements = [
    "CREATE INDEX idx_grid_voyage_network_track ON context_data.grid_voyage_network USING GIST (track);",
    "CREATE INDEX idx_grid_voyage_network_source ON context_data.grid_voyage_network(source_grid_id);",
    "CREATE INDEX idx_grid_voyage_network_target ON context_data.grid_voyage_network(target_grid_id);"
]

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Execute each SQL statement
    for sql in sql_statements:
        cur.execute(sql)
        print(f"Executed: {sql}")

    # Commit changes and close connection
    conn.commit()
    cur.close()
    conn.close()

    print("All indexes created successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
