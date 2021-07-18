import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn): 
    """
    This function drops the tables from database
    params:
        cur : psycopg2 cursor object
        conn: psycopg2 connection object
    return: None --> Void 
    """
    for query in drop_table_queries:
        try:
            # Execute query and commit the results
            cur.execute(query)
            conn.commit()
        except Exception as ex:
            print(f"Exception {str(ex)} occured while executing query {query}")


def create_tables(cur, conn):
    """
    This function creates the tables from database
    params:
        cur : psycopg2 cursor object
        conn: psycopg2 connection object
    return: None --> Void 
    """
    for query in create_table_queries:
        try:
            # Execute query and commit the results
            cur.execute(query)
            conn.commit()
        except Exception as ex:
            print(f"Exception {str(ex)} occured while executing query {query}")


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
    
    # Drops the tables if exists and then recreates those
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    # Closes the connection
    conn.close()


if __name__ == "__main__":
    # Calls the main() method to kickstart execution
    main()