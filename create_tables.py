import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def drop_tables(cur, conn):
    """
    Drop existing tables for a fresh start
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create both staging tables and tables for final star schema
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()