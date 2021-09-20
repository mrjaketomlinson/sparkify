import psycopg2
import json
from sql_queries import create_table_queries, drop_table_queries


def get_credentials():
    """
    Description: Gets database credentials from the json file in the project 
    used to login to the local Postgres database.
    
    Arguments:
        None
    
    Returns:
        creds: Dictionary with the username and password from the jason file.
    """
    with open('credentials.json') as f:
        creds = json.load(f)
    return creds


def create_database(creds):
    """
    Description: This function drops the sparify database if it exists and
    creates a fresh version of the database. It returns the cursor and
    connection object to the database.
    
    Arguments: 
        creds: A dictionary with two values, username and password, used to log
            in to the database.
    
    Returns:
        cur: The cursor object to the database.
        conn: The connection object to the database.
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
    Description: Drops each table using the queries in `drop_table_queries` list.
    
    Arguments:
        cur: The cursor object to the database.
        conn: The connection object to the database.
    
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Creates each table using the queries in `create_table_queries` list.
    
    Arguments:
        cur: The cursor object to the database.
        conn: The connection object to the database.
    
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: Gets credentials for database, drops (if exists) and creates 
    the sparkify database. Establishes connection with the sparkify database and
    gets cursor to it. Drops all the tables. Creates all tables needed. Finally,
    closes the connection. 
    
    Arguments:
        None
    
    Returns:
        None
    """
    creds = get_credentials()
    cur, conn = create_database(creds)
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()