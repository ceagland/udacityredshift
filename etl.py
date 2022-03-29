import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables from S3 buckets
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from newly-created staging tables into songplay star schema setup
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn_string="postgresql://{}:{}@{}:{}/{}".format(config.get('CLUSTER', 'DB_USER'),
                                                     config.get('CLUSTER', 'DB_PASSWORD'),
                                                     config.get('CLUSTER','HOST'),
                                                     config.get('CLUSTER', 'DB_PORT'),
                                                     config.get('CLUSTER','DB_NAME'))
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()