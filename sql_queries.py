import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
iam_role_arn=config.get('IAM_ROLE','ARN')
log_path=config.get('S3','LOG_JSONPATH')
log_data=config.get('S3','LOG_DATA')
song_data=config.get('S3','SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table (
                      entry_id INT IDENTITY(0,1) PRIMARY KEY,
                      artist VARCHAR,
                      auth VARCHAR,
                      firstName VARCHAR,
                      gender VARCHAR,
                      iteminSession SMALLINT,
                      lastName VARCHAR,
                      length DECIMAL(9,5),
                      level VARCHAR,
                      location VARCHAR,
                      method VARCHAR,
                      page VARCHAR,
                      registration NUMERIC,
                      session_id INT,
                      song VARCHAR,
                      status SMALLINT,
                      ts TIMESTAMPTZ,
                      userAgent VARCHAR,
                      user_id INT);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table (
                        num_songs SMALLINT,
                        artist_id VARCHAR,
                        artist_latitude NUMERIC,
                        artist_longitude NUMERIC,
                        artist_location VARCHAR,
                        artist_name VARCHAR,
                        song_id VARCHAR,
                        title VARCHAR,
                        duration DECIMAL(9,5),
                        year SMALLINT );""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (
                        songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                        start_time TIMESTAMPTZ NOT NULL, 
                        user_id INT, 
                        level VARCHAR, 
                        song_id VARCHAR,
                        artist_id VARCHAR, 
                        session_id VARCHAR, 
                        location VARCHAR, 
                        user_agent VARCHAR);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (
                    user_id VARCHAR PRIMARY KEY,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender VARCHAR, 
                    level VARCHAR);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (
                    song_id VARCHAR PRIMARY KEY, 
                    title VARCHAR, 
                    artist_id VARCHAR, 
                    year SMALLINT, 
                    duration DECIMAL(9,5));""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
                        artist_id VARCHAR PRIMARY KEY,
                        artist_name VARCHAR,
                        location VARCHAR,
                        latitude NUMERIC, 
                        longitude NUMERIC);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table(
                    start_time TIMESTAMPTZ PRIMARY KEY, 
                    hour SMALLINT, 
                    day SMALLINT, 
                    week SMALLINT,
                    month SMALLINT, 
                    year SMALLINT,
                    weekday Boolean);""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events_table FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        json {} compupdate off region 'us-west-2' 
                        TIMEFORMAT 'epochmillisecs';
                        """).format(log_data, iam_role_arn, log_path)

staging_songs_copy = ("""COPY staging_songs_table FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        json 'auto' compupdate off region 'us-west-2'
                        TIMEFORMAT 'epochmillisecs';
                    """).format(song_data, iam_role_arn)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table (start_time, user_id, level,song_id,artist_id, session_id, location, user_agent)
                            SELECT ts, user_id, level, song_id, artist_id, session_id,location, userAgent
                            FROM staging_events_table se
                            LEFT JOIN staging_songs_table ss
                            ON se.artist=ss.artist_name AND
                            se.length=ss.duration AND
                            se.song=ss.title;
""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id, firstName, lastName, gender, level
                        FROM staging_events_table 
                        where page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id,year, duration
                        FROM staging_songs_table
                        where song_id IS NOT NULL;               
""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id, artist_name, location, latitude, longitude)
                        SELECT artist_id, artist_name, artist_location,artist_latitude, artist_longitude
                        FROM staging_songs_table;                
""")

time_table_insert = ("""INSERT INTO time_table (start_time, hour, day ,week, month,year, weekday)
                        SELECT ts , 
                        EXTRACT (hour from ts) AS hour,
                        EXTRACT (day from ts) AS day,
                        EXTRACT (week from ts) AS week,
                        EXTRACT (month from ts) AS month,
                        EXTRACT (year from ts) AS year,
                        CASE WHEN EXTRACT(DOW FROM ts) NOT IN (0, 6) THEN true ELSE false END AS is_weekday
                        FROM staging_events_table;
                        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
