import happybase

# Create connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

# Open connection to perform operations
def open_connection():
    connection.open()

# Close the opened connection
def close_connection():
    connection.close()

# List all tables in HBase
def list_tables():
    print("Fetching all tables...")
    open_connection()
    tables = connection.tables()
    close_connection()
    print("All tables fetched.")
    return tables

# Create a table by passing name and column families as parameters
def create_table(name, column_families):
    print(f"Creating table {name}...")
    tables = list_tables()
    if name not in tables:
        open_connection()
        connection.create_table(name, column_families)
        close_connection()
        print(f"Table {name} created.")
    else:
        print(f"Table {name} already present.")

# Define column families for yellow_taxi_trips table
column_families = {
    'info': dict(max_versions=5),
}

# Create the table
create_table('yellow_taxi_trips', column_families)
