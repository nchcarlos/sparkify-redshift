from sql_queries import create_table_queries
from sql_queries import drop_table_queries

def drop_tables(cur, conn):
    """

    :param cur:
    :param conn:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, drop_tables_first=True):
    """

    :param cur:
    :param conn:
    :param drop_tables_first:
    """
    if drop_tables_first:
        drop_tables(cur, conn)

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    # config = configparser.ConfigParser()
    # config.read('dwh.cfg')

    # conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # cur = conn.cursor()

    # drop_tables(cur, conn)
    create_tables(None, None)

    # conn.close()


if __name__ == "__main__":
    main()