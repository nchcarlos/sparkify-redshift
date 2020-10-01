import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events"'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs"'

songplay_table_drop = 'DROP TABLE IF EXISTS "songplays"'
user_table_drop = 'DROP TABLE IF EXISTS "users"'
song_table_drop = 'DROP TABLE IF EXISTS "songs"'
artist_table_drop = 'DROP TABLE IF EXISTS "artists"'
time_table_drop = 'DROP TABLE IF EXISTS "time"'

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events" (
    "artist"            TEXT,
    "auth"              TEXT,
    "firstName"         TEXT,
    "gender"            CHAR,
    "itemInSession"     INT,
    "lastName"          TEXT,
    "length"            FLOAT,
    "level"             TEXT,
    "location"          TEXT,
    "method"            TEXT,
    "page"              TEXT,
    "registration"      FLOAT,
    "sessionId"         INT,
    "song"              TEXT,
    "status"            INT,
    "ts"                INT,
    "userAgent"         TEXT,
    "userId"            INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs"         INT,
    "artist_id"         TEXT,
    "artist_latitude"   FLOAT,
    "artist_longitude"  FLOAT,
    "artist_location"   TEXT,
    "artist_name"       TEXT,
    "song_id"           TEXT,
    "title"             TEXT,
    "duration"          FLOAT,
    "year"              INT
);
""")

songplay_table_create = """
CREATE TABLE "songplays"
(
    "songplay_id"   INT IDENTITY(0,1) PRIMARY KEY,
    "start_time"    TIMESTAMP,
    "user_id"       INT,
    "level"         TEXT,
    "song_id"       TEXT,
    "artist_id"     TEXT,
    "session_id"    INT,
    "location"      TEXT,
    "user_agent"    TEXT
)
"""

user_table_create = """
CREATE TABLE "users"
(
    "user_id"       INT PRIMARY KEY,
    "first_name"    TEXT,
    "last_name"     TEXT,
    "gender"        TEXT,
    "level"         TEXT
)
"""

song_table_create = """
CREATE TABLE "songs"
(
    "song_id"   TEXT PRIMARY KEY,
    "title"     TEXT,
    "artist_id" TEXT,
    "year"      INT,
    "duration"  FLOAT
)
"""

artist_table_create = """
CREATE TABLE "artists"
(
    "artist_id" TEXT PRIMARY KEY,
    "name"      TEXT,
    "location"  TEXT,
    "latitude"  FLOAT,
    "longitude" FLOAT
)
"""

time_table_create = """
CREATE TABLE "time"
(
    "start_time"    TIMESTAMP PRIMARY KEY,
    "hour"          INT,
    "day"           INT,
    "week"          INT,
    "month"         INT,
    "year"          INT,
    "weekday"       INT
)
"""

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy,
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
