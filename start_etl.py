import create_tables, etl


def main():
    '''
    Shorthand to execute table drop, create and subsequent ETL.
    :return:
    '''
    create_tables.main()
    print("Job done: Tables created.")
    etl.main()
    print("Job done: ETL finished.")
    
    
if __name__ == '__main__':
    main()