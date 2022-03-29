import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
    (artist varchar,
     auth varchar(12),
     firstName varchar,
     gender varchar(1),
     itemInSession int,
     lastName varchar,
     length float,
     level varchar(8),
     location varchar,
     method varchar(8),
     page varchar,
     registration float,
     sessionId int,
     song varchar,
     status int,
     ts bigint,
     userAgent varchar,
     userId int     
     )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
    (num_songs int,
     artist_id varchar,
     artist_latitude float,
     artist_longitude float,
     artist_location varchar,
     artist_name varchar,
     song_id varchar,
     title varchar,
     duration float,
     year int
     )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (songplay_id int IDENTITY(0,1) DISTKEY,
                                       start_time timestamp NOT NULL,
                                       user_id varchar NOT NULL,
                                       level varchar NOT NULL,
                                       song_id varchar,
                                       artist_id varchar,
                                       session_id varchar NOT NULL,
                                       location varchar NOT NULL,
                                       user_agent varchar NOT NULL,
                                       PRIMARY KEY (songplay_id))
                                       SORTKEY(start_time, session_id)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (user_id varchar NOT NULL SORTKEY,
                               first_name varchar NOT NULL,
                               last_name varchar NOT NULL,
                               gender varchar,
                               level varchar NOT NULL,
                               PRIMARY KEY (user_id))
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (song_id varchar NOT NULL DISTKEY,
                               title varchar NOT NULL,
                               artist_id varchar NOT NULL,
                               year int SORTKEY,
                               duration float,
                               PRIMARY KEY (song_id))                       
""")
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (artist_id varchar NOT NULL,
                               name varchar NOT NULL,
                               location varchar,
                               latitude float,
                               longitude float,
                               PRIMARY KEY (artist_id))
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (start_time timestamp NOT NULL SORTKEY,
                               hour int NOT NULL,
                               day int NOT NULL,
                               week int NOT NULL,
                               month int NOT NULL,
                               year int NOT NULL,
                               weekday int NOT NULL,
                               PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    compupdate off
    region 'us-west-2';
""").format(config.get('S3', 'log_data'), config.get('IAM', 'RoleArn'), config.get('S3', 'log_json'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    compupdate off
    region 'us-west-2';
""").format(config.get('S3', 'song_data'), config.get('IAM', 'RoleArn'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
                TIMESTAMP 'epoch' + ts * INTERVAL '0.001 seconds' as starttime,
                userid,
                level,
                staging_songs.song_id,
                staging_songs.artist_id,
                sessionid,
                location,
                useragent
                FROM staging_events
                INNER JOIN staging_songs
                ON staging_songs.title = staging_events.song
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
                userid,
                firstname,
                lastname,
                gender,
                level
    FROM staging_events
    WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
                song_id,
                title,
                artist_id,
                year,
                duration
               FROM staging_songs
               WHERE song_id IS NOT NULL;    
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time,
        extract(hour from start_time) as hour,
        extract(day from start_time) as day,
        extract(week from start_time) as week,
        extract(month from start_time) as month,
        extract(year from start_time) as year,
        extract(weekday from start_time) as weekday
    FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
