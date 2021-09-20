import os
import glob
import psycopg2
import pandas as pd
import datetime
from create_tables import get_credentials
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function is responsible for processing song_file data. The
    function inserts records into the songs and artists tables.
    
    Arguments:
        cur: The cursor object to the database
        filepath: The song_file filepath to be processed.
        
    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function is responsible for processing log_file data. The 
    function filters the data in the file, and adds records to the time, users,
    and songplays table.
    
    Arguments:
        cur: The cursor object to the database.
        filepath: The log_file filepath to be processed.
    
    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    df['ts_datetime'] = df['ts'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0))
    df['userId'] = df['userId'].apply(lambda x: int(x))

    # convert timestamp column to datetime
    t = df['ts_datetime']
    
    # insert time data records
    time_data = time_data = {
        'timestamp': t,
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week': t.dt.isocalendar().week,
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday
    }
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts_datetime, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: Connects to the database. Processes all data in the song_data
    folder. Processes all data in the log_data folder. Closes the connection.
    
    Arguments:
        None
    
    Returns:
        None
    """
    creds = get_credentials()
    conn = psycopg2.connect(f'host=localhost dbname=sparkifydb user={creds["username"]} password={creds["password"]}')
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()