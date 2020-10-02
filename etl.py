import boto3
import pandas as pd
import psycopg2

import cluster
import config
import dwh_user
from sql_queries import copy_table_queries
from sql_queries import insert_table_queries
import util

def process_song_data(cur, conn):
    """
    Parse the staging_songs table and insert the data into the songs and
    artists tables.

    :param cur: a cursor to perform the database operations
    :param conn: a connection to the database
    """
    cur.execute(insert_table_queries['songs'])
    cur.execute(insert_table_queries['artists'])
    conn.commit()

def process_log_data(cur, conn):
    """
    Parse the staging_events table and insert the data into the songplays,
    users, and time databases.

    :param cur: a cursor to perform the database operations
    :param conn: a connection to the database
    """
    sql = 'select * from staging_events;'
    df = pd.read_sql(sql, conn)
    # print(df.shape)

    # we have to handle missing values for userId
    df['userid'] = df['userid'].apply(lambda x: None if isinstance(x, str) else x)

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['start_time'] = t.dt.time
    df['hour'] = t.dt.hour
    df['day'] = t.dt.day
    df['week'] = t.dt.isocalendar().week
    df['month'] = t.dt.month
    df['year'] = t.dt.year
    df['weekday'] = t.dt.weekday

    # filter by NextSong action
    songplay_data = df[df['page'] == 'NextSong']

    songplay_cols = [
        'ts',
        'userid',
        'level',
        'sessionid',
        'location',
        'useragent',
        'song',
        'artist',
    ]

    time_cols = [
        'ts',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    ]

    user_cols = [
        'userid',
        'firstname',
        'lastname',
        'gender',
        'level'
    ]

    # load user table, records where userId has a value
    user_df = df[df['userid'].notnull()]

    # insert songplay records
    # sub-queries are used in the songplay_table_insert SQL to get the song_id
    # and the artist_id so we don't have to execute extra select statements to
    # get the data
    songplay_df = songplay_data[songplay_cols]
    for _, row in songplay_df.iterrows():
        # add an extra artist field for the query
        vals = tuple([*row.values, row.values[-2]])
        cur.execute(insert_table_queries['songplays'], vals)
        conn.commit()
        pass

    # handle duplicate records
    time_df = df[time_cols].drop_duplicates(subset=['ts'])
    user_df = user_df[user_cols].drop_duplicates(subset=['userid'], keep='last')

    # insert time data records
    for _, row in time_df.iterrows():
        cur.execute(insert_table_queries['time'], tuple(row))
        conn.commit()

    # insert user records
    for _, row in user_df[user_cols].iterrows():
        cur.execute(insert_table_queries['users'], tuple(row.values))
        conn.commit()

def load_staging_tables(cur, conn, cfg):
    """
    Load the data from S3 into the staging tables.

    :param cur: a cursor to perform the database operations
    :param conn: a connection to the database
    :param cfg: a config object for the project
    """
    role = dwh_user.get_role_arn(util.get_iam_client(cfg), cfg.IAM_ROLE_NAME)
    for query in copy_table_queries:
        cur.execute(query.format(role))
        conn.commit()

def insert_tables(cur, conn):
    """
    Process the data in the staging tables and load them in to the appropriate
    data warehouse tables.

    :param cur: a cursor to perform the database operations
    :param conn: a connection to the database
    """
    # process_song_data(cur, conn)
    process_log_data(cur, conn)

def main():
    cfg = config.get_config()
    conn = cluster.get_connection(cfg)
    cur = conn.cursor()

    load_staging_tables(cur, conn, cfg.IAM_ROLE_NAME)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
