import mariadb
import os

# Establish a connection to the MariaDB database
db = mariadb.connect(
    host=os.getenv("DB_HOST", "mariadb"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", os.getenv("MARIADB_ROOT_PASSWORD", "teste123")),
    database=os.getenv("DB_NAME", "iscte_spot"),
)

cursor = db.cursor()

def drop_all_tables():
    # Query to get all table names
    tables = ['Sales', 'Clients', 'Products', 'Companies', 'SupportTickets', 'Users']
    # Drop each table
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"Table {table} dropped.")

    db.commit()

# Run the drop tables function
drop_all_tables()

# Close the connection
cursor.close()
db.close()
