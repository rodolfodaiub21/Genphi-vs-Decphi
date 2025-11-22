import mysql.connector
import pandas as pd
import sys
from mysql.connector import Error

def create_connection(host, user, password, database=None):
    try:
        if database:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
        else:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password
            )
        print("MySQL connection successful")
        return connection
    except Error as e:
        # If a database was specified but doesn't exist (error 1049),
        # connect without a database so we can create it.
        err_no = getattr(e, 'errno', None)
        if database and err_no == 1049:
            print(f"Database '{database}' not found. Connecting without database to create it.")
            try:
                connection = mysql.connector.connect(
                    host=host,
                    user=user,
                    password=password
                )
                print("MySQL connection successful (no database)")
                return connection
            except Error as e2:
                print(f"Error connecting without database: '{e2}'")
                return None

        print(f"Error: '{e}'")
        return None

def create_database(connection, dbname):
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
        print(f"Database '{dbname}' created or already exists.")
    except Error as e:
        print(f"Error creating database: '{e}'")
    finally:
        cursor.close()

def create_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS phishing_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        phishing_data LONGTEXT NOT NULL,
        phishing_type ENUM('mail', 'url') NOT NULL,
        phishing_class TINYINT(1) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_type (phishing_type),
        INDEX idx_class (phishing_class),
        INDEX idx_type_class (phishing_type, phishing_class)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'phishing_data' created successfully")
    except Error as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()

def upload_csv_to_mysql(csv_file, host, user, password, database, batch_size=1000):
    print(f"Reading CSV file: {csv_file}")

    try:
        df = pd.read_csv(csv_file)
        print(f"CSV loaded successfully. Total rows: {len(df)}")
    except Exception as e:
        print(f"Error reading CSV: '{e}'")
        return

    # Normalize column names: strip, lowercase, replace spaces with underscores
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    required_columns = ['phishing_data', 'phishing_type', 'phishing_class']
    if not all(col in df.columns for col in required_columns):
        print(f"Error: CSV must contain columns: {required_columns}")
        print(f"Found columns: {df.columns.tolist()}")
        return

    # Drop rows missing required fields
    before_drop = len(df)
    df = df.dropna(subset=required_columns)
    after_drop = len(df)
    if after_drop < before_drop:
        print(f"Dropped {before_drop - after_drop} rows with missing required values.")

    # Ensure phishing_class is integer-like; coerce and drop invalid
    df['phishing_class'] = pd.to_numeric(df['phishing_class'], errors='coerce')
    before = len(df)
    df = df.dropna(subset=['phishing_class'])
    df['phishing_class'] = df['phishing_class'].astype(int)
    after = len(df)
    if after < before:
        print(f"Dropped {before - after} rows with invalid 'phishing_class' values.")

    # Normalize phishing_type values
    df['phishing_type'] = df['phishing_type'].astype(str).str.strip().str.lower()
    
    connection = create_connection(host, user, password, database)
    if not connection:
        return
    
    create_database(connection, database)
    # Close and reconnect specifying the database so subsequent operations use it
    try:
        connection.close()
    except Exception:
        pass

    connection = create_connection(host, user, password, database)
    if not connection:
        print("Failed to connect to the newly created database.")
        return

    create_table(connection)

    cursor = connection.cursor()
    insert_query = """
        INSERT INTO phishing_data (phishing_data, phishing_type, phishing_class)
        VALUES (%s, %s, %s)
    """

    total_rows = len(df)
    inserted = 0

    try:
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            data_tuples = [
                (row['phishing_data'], row['phishing_type'], int(row['phishing_class']))
                for _, row in batch.iterrows()
            ]

            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            inserted += len(data_tuples)

            print(f"Progress: {inserted}/{total_rows} rows inserted ({inserted/total_rows*100:.1f}%)")
        
        print(f"\nData upload completed successfully! Total rows inserted: {inserted}")
    
    except Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print("MySQL connection closed")


if __name__ == "__main__":
    CSV_FILE = ""
    MYSQLHOST = ""
    MYSQLUSER = ""
    MYSQLPASSWORD = ""
    MYSQLDB = ""
    BATCH_SIZE = 1000

    upload_csv_to_mysql(
        csv_file=CSV_FILE,
        host=MYSQLHOST,
        user=MYSQLUSER,
        password=MYSQLPASSWORD,
        database=MYSQLDB,
        batch_size=BATCH_SIZE
    )