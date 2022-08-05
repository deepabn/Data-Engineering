import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Function to copy data to staging tables from AWS S3 bucket 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Function to insert records to fact and dimensional tables from staging tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    -Establishes connection with the sparkify database created in AWS Redshift and gets
    cursor to it.
    
    -Loads staging tables with contents from 'udacity-dend' s3 bucket 
    
    -Inserts records from staging tables to fact and dimensional tables
    
    -Finally, closes the connection
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