import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy data from S3 json file to the staging tables.
    
    Parameters:
      cur - cursor.
      conn - connection to the database.
      
    Returns: None.  
    """   
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert records to the respective tables from the staging tables.
    
    Parameters:
      cur - cursor.
      conn - connection to the database.
      
    Returns: None.  
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with the database.
    - Load staging tables.
    - Insert records to the tables from the staging tables.
    
    Parameters:
      N/A
      
    Returns: None.  
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()