import psycopg2
import json
from sql_queries import create_table_queries, drop_table_queries


def get_credentials():
    """
    Returns a dictionary with the username and password from json file to
    connect to PostgreSQL Database
    """
    with open('credentials.json') as f:
        creds = json.load(f)
    return creds


def create_database(creds):
    """
    - creds (dictionary): includes username and password from get_credentials function
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect(f'host=localhost dbname=udacity user={creds["username"]} password={creds["password"]}')
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(f'host=localhost dbname=sparkifydb user={creds["username"]} password={creds["password"]}')
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Gets credentials for database
    
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    creds = get_credentials()
    cur, conn = create_database(creds)
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()