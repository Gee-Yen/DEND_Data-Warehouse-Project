import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_events
(
  artist_name VARCHAR(100),
  auth VARCHAR(25),
  user_first_name VARCHAR(50),
  user_gender CHAR(1),
  iteminsession INTEGER,
  user_last_name VARCHAR(50),
  song_length REAL,
  level VARCHAR(10),
  location TEXT,
  method VARCHAR(10),
  page VARCHAR(10),
  registration REAL,
  sessionid INTEGER,
  song_title varchar(100),
  status INTEGER,
  ts TIMESTAMP,
  user_agent TEXT,
  user_id INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_songs
(
  num_songs INTEGER,
  artist_id VARCHAR(50),
  artist_latitude VARCHAR(10),
  artist_longitude VARCHAR(10),
  artist_location VARCHAR(50),
  artist_name VARCHAR(100),
  song_id VARCHAR(50),
  title VARCHAR(100),
  duration REAL,
  year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
( 
  songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
  start_time TIMESTAMP NOT NULL REFERENCES time(start_time) DISTKEY, 
  user_id INTEGER NOT NULL REFERENCES users(user_id), 
  level VARCHAR(10), 
  song_id VARCHAR(50) REFERENCES songs(song_id) SORTKEY, 
  artist_id VARCHAR(50) REFERENCES artists(artist_id), 
  session_id INTEGER, 
  location TEXT, 
  user_agent TEXT
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
( 
  user_id INTEGER PRIMARY KEY SORTKEY, 
  first_name VARCHAR(50), 
  last_name VARCHAR(50), 
  gender CHAR(1), 
  level VARCHAR(10)
)
DISTSTYLE all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
( 
  song_id VARCHAR(50) PRIMARY KEY SORTKEY, 
  title VARCHAR(100), 
  artist_id VARCHAR(50), 
  year INTEGER, 
  duration REAL
)
DISTSTYLE all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
( 
  artist_id VARCHAR(50) PRIMARY KEY SORTKEY, 
  name VARCHAR(100), 
  location TEXT, 
  latitude VARCHAR(10), 
  longitude VARCHAR(10)
)
DISTSTYLE all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
( 
  start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY, 
  hour INTEGER, 
  day INTEGER, 
  week INTEGER, 
  month INTEGER, 
  year INTEGER, 
  weekday INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY {} from {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON {};
""").format('stg_events', config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
COPY {} from {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON 'auto';
""").format('stg_songs', config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time , user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts,
       e.user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionid,
       e.location,
       e.user_agent
FROM   stg_events e
JOIN   stg_songs s
ON     e.artist_name = s.artist_name
WHERE  e.song_length = s.duration
AND    e.song_title  = s.title
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT e.user_id,
       e.user_first_name,
       e.user_last_name,
       e.user_gender,
       e.level
FROM   stg_events e
WHERE  (e.user_id, e.ts) IN (
                              SELECT user_id,
                                     MAX(ts) max_ts
                              FROM   stg_events 
                              GROUP BY user_id
                            )
;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
select s.song_id,
       s.title,
       s.artist_id,
       s.year,
       s.duration
FROM   stg_songs s
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT s.artist_id,
       s.artist_name,
       s.artist_location,
       s.artist_latitude,
       s.artist_longitude
FROM   stg_songs s
WHERE  (s.artist_id, s.year) IN (
                                  SELECT artist_id,
                                         MAX(year) max_year
                                  FROM   stg_songs 
                                  GROUP BY artist_id
                                )
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT e.ts,
       EXTRACT(h FROM ts),
       EXTRACT(d FROM ts),
       EXTRACT(w FROM ts),
       EXTRACT(mon FROM ts),
       EXTRACT(y FROM ts),
       EXTRACT(dow FROM ts)
FROM   stg_events e
""")

# QUERY LISTS
# I moved the songplay_table_create variable to the end in the create_table_queries list
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
