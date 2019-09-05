import configparser
import psycopg2
import sys
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Loading data from S3 into staging tables.
    :param cur:
    :param conn:
    :return:
    '''
    error = "none"
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error on loading table. Current query: {query}")
            print(e)
            error = 'error'
            conn.close()
            sys.exit(1)

    print(f"Finished: tables loaded. Error status: {error}")


def insert_tables(cur, conn):
    '''
    Insert data from staging tables into final tables.
    :param cur:
    :param conn:
    :return:
    '''
    error = "none"
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error on inserting table. Current query: {query}")
            print(e)
            error = 'error'
            conn.close()
            sys.exit(1)

    print(f"Finished: tables inserted. Error status: {error}")


def main():
    '''
    Execute ELT for s3 -> staging -> final
    :return:
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(config['ETL']['DWH_DB_USER'], config['ETL']['DWH_DB_PASSWORD'],
                                                       config['ETL']['DWH_ENDPOINT'], config['ETL']['DWH_PORT'],
                                                       config['ETL']['DWH_DB'])
    conn = psycopg2.connect(conn_string)

    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()