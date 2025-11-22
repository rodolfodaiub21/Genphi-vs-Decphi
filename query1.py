import mysql.connector
from mysql.connector import Error
import pandas as pd

def create_connection(host, user, password, database):
    """Create a database connection"""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        print("MySQL connection successful\n")
        return connection
    except Error as e:
        print(f"Error: '{e}'")
        return None

def query_phishing_distribution(connection):
    """Query the distribution of phishing classes by type"""
    query = """
    SELECT 
        phishing_type,
        phishing_class,
        COUNT(*) as count
    FROM phishing_data
    GROUP BY phishing_type, phishing_class
    ORDER BY phishing_type, phishing_class;
    """
    
    try:
        # Execute query and load into DataFrame
        df = pd.read_sql(query, connection)
        
        # Display results
        print("=" * 60)
        print("PHISHING DATA DISTRIBUTION")
        print("=" * 60)
        print(df.to_string(index=False))
        print("=" * 60)
        
        # Create pivot table for better visualization
        print("\nPIVOT TABLE VIEW:")
        print("=" * 60)
        pivot = df.pivot(index='phishing_type', columns='phishing_class', values='count').fillna(0).astype(int)
        pivot.columns = [f'Class_{int(col)}' for col in pivot.columns]
        pivot['Total'] = pivot.sum(axis=1)
        print(pivot)
        print("=" * 60)
        
        # Summary statistics
        print("\nSUMMARY:")
        print("=" * 60)
        total_records = df['count'].sum()
        print(f"Total Records: {total_records:,}")
        print()
        for ptype in df['phishing_type'].unique():
            type_data = df[df['phishing_type'] == ptype]
            type_total = type_data['count'].sum()
            print(f"{ptype.upper()}:")
            for _, row in type_data.iterrows():
                percentage = (row['count'] / type_total) * 100
                print(f"  Class {int(row['phishing_class'])}: {int(row['count']):>10,} ({percentage:>5.2f}%)")
            print(f"  Total:       {type_total:>10,}")
            print()
        print("=" * 60)
        
        return df
        
    except Error as e:
        print(f"Error executing query: '{e}'")
        return None

if __name__ == "__main__":
    # Configuration
    MYSQL_HOST = ""
    MYSQL_USER = ""
    MYSQL_PASSWORD = ""
    MYSQL_DATABASE = ""
    
    # Connect to database
    connection = create_connection(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)
    
    if connection:
        # Execute query
        results = query_phishing_distribution(connection)
        
        # Close connection
        connection.close()
        print("\nMySQL connection closed")