import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS  songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# Staging Tables

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        id             BIGINT IDENTITY(0,1)   PRIMARY KEY,
        artist         VARCHAR,
        auth           VARCHAR,
        firstName      VARCHAR,
        gender         VARCHAR(3),
        itemInSession  SMALLINT,
        lastName       VARCHAR,
        length         FLOAT,
        level          VARCHAR,
        location       VARCHAR,
        method         VARCHAR(3),
        page           VARCHAR,
        registration   FLOAT,
        sessionId      BIGINT,
        song           VARCHAR,
        status         SMALLINT,
        ts             BIGINT,
        userAgent      VARCHAR,
        userId         INTEGER
    );
""")

# {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        id               INTEGER IDENTITY(0,1)   PRIMARY KEY,
        num_songs        INTEGER,
        artist_id        VARCHAR,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         FLOAT,
        year             SMALLINT
    );
""")

# INSERT DATA INTO STAGING TABLES

staging_events_copy = (
    f"COPY staging_events " 
    f"FROM {LOG_DATA} " 
    f"CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}' " 
    f"FORMAT as JSON {LOG_JSONPATH} " 
    f"STATUPDATE ON " 
    f"region 'us-west-2';"
)

staging_songs_copy = (
    f"COPY staging_songs FROM {SONG_DATA} " 
    f"CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}' " 
    f"FORMAT as JSON 'auto' " 
    f"ACCEPTINVCHARS AS '^' " 
    f"STATUPDATE ON " 
    f"region 'us-west-2';"
)


# CREATE FINAL TABLE

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
         user_id         INTEGER       PRIMARY KEY,
         first_name      VARCHAR       NOT NULL,
         last_name       VARCHAR       NOT NULL,
         gender          VARCHAR(4),
         level           VARCHAR(20)
     ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
         song_id         VARCHAR       PRIMARY KEY,
         title           VARCHAR       NOT NULL,
         artist_id       VARCHAR       NOT NULL DISTKEY SORTKEY,
         year            SMALLINT,
         duration        FLOAT
     );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
         artist_id       VARCHAR       PRIMARY KEY,
         name            VARCHAR       NOT NULL,
         location        VARCHAR,
         latitude        FLOAT,
         longitude       FLOAT
     ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
         start_time      BIGINT        PRIMARY KEY SORTKEY,
         hour            SMALLINT      NOT NULL,
         day             SMALLINT      NOT NULL,
         week            SMALLINT      NOT NULL,
         month           SMALLINT      NOT NULL,
         year            SMALLINT      NOT NULL,
         week_day        SMALLINT       NOT NULL
     ) diststyle all;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        id               BIGINT        IDENTITY(0,1) PRIMARY KEY,
        start_time       BIGINT        NOT NULL REFERENCES time(start_time) SORTKEY,
        user_id          INTEGER       NOT NULL REFERENCES users(user_id),
        level            VARCHAR       NOT NULL,
        song_id          VARCHAR       NOT NULL REFERENCES songs(song_id) DISTKEY,
        artist_id        VARCHAR       NOT NULL REFERENCES artists(artist_id) ,
        session_id       BIGINT        NOT NULL,
        location         VARCHAR,
        user_agent       VARCHAR
     );
""")


# INSERT INTO FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) 
    SELECT DISTINCT
        se.ts                 AS start_time,
        se.userId             AS user_id,
        se.level,
        so.song_id            AS song_id,
        se.artist             AS artist_id,
        se.sessionId          AS session_id,
        se.location,
        se.userAgent          AS user_agent
    FROM staging_events se, staging_songs so 
    WHERE se.song = so.title AND page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        userId              AS user_id,
        firstName           AS first_name,
        lastName            AS last_name,
        gender              AS gender,
        level               AS level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name         AS name,
        artist_location     AS location,        
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        week_day
    )
    SELECT ts as start_time,
           EXTRACT(hour FROM date_string)     AS hour,
           EXTRACT(day FROM date_string)      AS day,
           EXTRACT(week FROM date_string)     AS week,
           EXTRACT(month FROM date_string)    AS month,
           EXTRACT(year FROM date_string)     AS year,
           EXTRACT(weekday FROM date_string) AS week_day
    FROM ( SELECT DISTINCT 
               ts, 
               '1970-01-01'::date + ts / 1000 * INTERVAL '1 second' AS date_string 
           FROM staging_events 
           WHERE page = 'NextSong')
    ORDER BY start_time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
