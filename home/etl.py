import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads data into staging_tables
    params:
        cur : psycopg2 cursor object
        conn: psycopg2 connection object
    return: None --> Void 
    """
    for query in copy_table_queries:
        try:
            # Execute query and commit the results
            cur.execute(query)
            conn.commit()
        except Exception as ex:
            print(f"Exception {str(ex)} occured while loading staging table {query}")


def insert_tables(cur, conn):
    """
    This function loads data into regular tables after doing some preprocessing
    params:
        cur : psycopg2 cursor object
        conn: psycopg2 connection object
    return: None --> Void 
    """
    
    for query in insert_table_queries:
        try:
            # Execute query and commit the results
            cur.execute(query)
            conn.commit()
        except Exception as ex:
            print(f"Exception {str(ex)} occured while inserting data. Query is {query}")


def main():
    """
    Main method where the execution begins
    params : None
    return: None
    """
    
    # Read redshit & aws credential and cluster details
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connect to redshift cluster based on the credentials & details provided in dwh.cfg file
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Loads the staging_table first and the the regular tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    # Closes the connection
    conn.close()


if __name__ == "__main__":
    # Calls the main() method to kickstart execution
    main()