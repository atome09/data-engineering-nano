# DROP TABLES

songplay_table_drop = 'DROP TABLE IF EXISTS songplays;'
user_table_drop = 'DROP TABLE IF EXISTS users;'
song_table_drop = 'DROP TABLE IF EXISTS songs;'
artist_table_drop = 'DROP TABLE IF EXISTS artists;'
time_table_drop = 'DROP TABLE IF EXISTS time;'

# CREATE TABLES

user_table_create = \
    """CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY
                                         ,first_name VARCHAR
                                         ,last_name VARCHAR
                                         ,gender VARCHAR
                                         ,level VARCHAR);
    """

song_table_create = \
    """CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY
                                         ,title VARCHAR
                                         ,artist_id VARCHAR
                                         ,year INT
                                         ,duration DECIMAL);
    """

artist_table_create = \
    """CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY
                                           ,name VARCHAR
                                           ,location VARCHAR
                                           ,latitude NUMERIC
                                           ,longitude NUMERIC);
    """

time_table_create = \
    """CREATE TABLE IF NOT EXISTS time (time_id SERIAL PRIMARY KEY
                                        ,star_time TIME
                                        ,hour INT
                                        ,day INT
                                        ,week INT
                                        ,month INT
                                        ,year INT
                                        ,weekday INT);
    """

songplay_table_create = \
    """CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY
                                             ,start_time BIGINT NOT NULL
                                             ,user_id INT NOT NULL
                                             ,level VARCHAR
                                             ,song_id VARCHAR
                                             ,artist_id VARCHAR
                                             ,session_id VARCHAR
                                             ,location VARCHAR 
                                             ,user_agent VARCHAR
                                             );
            ALTER TABLE songplays ADD FOREIGN KEY(artist_id) REFERENCES artists(artist_id);
            ALTER TABLE songplays ADD FOREIGN KEY(song_id) REFERENCES songs(song_id);
    """

# INSERT RECORDS

song_table_insert = \
    ("""INSERT INTO songs (song_id
                          ,title
                          ,artist_id
                          ,year
                          ,duration
                          ) 
                   VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (song_id) 
                   DO NOTHING;
    """)

artist_table_insert = \
    ("""INSERT INTO artists (artist_id
                            ,name
                            ,location
                            ,latitude
                            ,longitude
                            ) 
                     VALUES (%s,%s,%s,%s,%s)
                     ON CONFLICT (artist_id) 
                     DO NOTHING;
    """)

user_table_insert = \
    ("""INSERT INTO users (user_id
                          ,first_name
                          ,last_name
                          ,gender
                          ,level) 
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET level=EXCLUDED.level
    """)

# FIND SONGS

song_select = \
    """SELECT s.song_id,a.artist_id
                    FROM artists a INNER JOIN songs s 
                        ON a.artist_id = s.artist_id 
                    WHERE s.title = %s AND a.name = %s AND s.duration = %s
    """

# QUERY LISTS

create_table_queries = [user_table_create,song_table_create, 
                        artist_table_create,time_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]
