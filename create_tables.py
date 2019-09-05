import configparser
import psycopg2
import sys
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Executes table drops for a given db connection and an associated cursor.
    Drop statements are defined in sql_queries drop_table_queries.
    :param cur:
    :param conn:
    :return:
    '''
    error = "none"
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error on dropping table. Current query: {query}")
            print(e)
            error = 'error'
            conn.close()
            sys.exit(1)

    print(f"Finished: tables dropped. Error status: {error}")


def create_tables(cur, conn):
    '''
    Executes table create for a given db connection and an associated cursor.
    Create statements are defined in sql_queries create_table_queries.
    :param cur:
    :param conn:
    :return:
    '''
    error = "none"
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error on creating tables. Current query: {query}")
            print(e)
            error = 'error'
            conn.close()
            sys.exit(1)

    print(f"Finished: tables created. Error status: {error}")


def main():
    '''
    Execute drop and create tables as prepartion for ETL.
    :return:
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')


    conn_string = "postgresql://{}:{}@{}:{}/{}".format(config['ETL']['DWH_DB_USER'], config['ETL']['DWH_DB_PASSWORD'],
                                                       config['ETL']['DWH_ENDPOINT'], config['ETL']['DWH_PORT'],
                                                       config['ETL']['DWH_DB'])
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()