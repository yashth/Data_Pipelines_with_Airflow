class SqlQueries:
    
    songplays_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    create_table_artists = ("""CREATE TABLE IF NOT EXISTS public.artists (
                            artistid        varchar(256)    NOT NULL,
                            name            varchar(256),
                            location        varchar(256),
                            lattitude       numeric(18,0),
                            longitude       numeric(18,0) 
                             )""")

    create_table_songplays = ("""CREATE TABLE IF NOT EXISTS public.songplays (
                                 songplay_id     bigint     IDENTITY(0,1) PRIMARY KEY, 
                                start_time      timestamp   NOT NULL, 
                                user_id         int         NOT NULL, 
                                level           varchar, 
                                song_id         varchar, 
                                artist_id       varchar, 
                                session_id      int, 
                                location        varchar, 
                                user_agent      varchar
                                )""")

    create_table_songs = ("""CREATE TABLE IF NOT EXISTS public.songs (
                            songid      varchar(256)    NOT NULL,
                            title       varchar(256),
                            artistid    varchar(256),
                            "year"      int4,
                            duration    numeric(18,0)
                            )""")
    
    create_table_time = ("""CREATE TABLE IF NOT EXISTS public.time (
                        start_time  timestamp, 
                        hour        int4, 
                        day         int4, 
                        week        int4, 
                        month       int4, 
                        year        int4, 
                        weekday     varchar(256)
                        )""")


    create_table_staging_events = ("""CREATE TABLE IF NOT EXISTS public.staging_events (
                                        artist          varchar(256),
                                        auth            varchar(256),
                                        firstname       varchar(256),
                                        gender          varchar(256),
                                        iteminsession   int4,
                                        lastname        varchar(256),
                                        length          numeric(18,0),
                                        "level"         varchar(256),
                                        location        varchar(256),
                                        "method"        varchar(256),
                                        page            varchar(256),
                                        registration    numeric(18,0),
                                        sessionid       int4,
                                        song            varchar(256),
                                        status          int4,
                                        ts              int8,
                                        useragent       varchar(256),
                                        userid          int4
                                        )""")



    create_table_staging_songs = ("""CREATE TABLE IF NOT EXISTS public.staging_songs (
                                    num_songs           int4,
                                    artist_id           varchar(256),
                                    artist_name         varchar(256),
                                    artist_latitude     numeric(18,0),
                                    artist_longitude    numeric(18,0),
                                    artist_location     varchar(256),
                                    song_id             varchar(256),
                                    title               varchar(256),
                                    duration            numeric(18,0),
                                    "year"              int4
                                )""")

    create_table_users = ("""CREATE TABLE IF NOT EXISTS public.users (
                            userid          int4            NOT NULL,
                            first_name      varchar(256),
                            last_name       varchar(256),
                            gender          varchar(256),
                            "level"         varchar(256)
                            )""")
    




