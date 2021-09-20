# Imports
import streamlit as st
import pandas as pd
import os
import glob
import json
import time
import datetime
import matplotlib.pyplot as plt
import numpy as np

# Page Layout
st.set_page_config(
    layout='wide',
    page_title='Sparkify',
    page_icon='random'
)

# Title and description
st.title('Sparkify')
st.markdown("""
Sparkify, a pretend startup, wants to analyze data on their music streaming app. What songs are people listening to? What artists get the most play time? What type of users are listening to the most music? In order to answer those questions, this project takes JSON files full of data and transforms it into a Postgres database full of tables worth querying. This [streamlit](https://streamlit.io) app displays the data in the project with some simple queries to answer group and display the data. 
""")

# About
expander_bar = st.expander('About')
expander_bar.markdown("""
* **Python libraries**: pandas, streamlit, matplotlib
* **Data Sources**:
    * **song_data**: A subset of the [Million Song Dataset](http://millionsongdataset.com), nested in JSON files. Each file contains metadata about a song and the artist of that song.
    * **log_data**: JSON files generated by [eventsim](https://github.com/Interana/eventsim), which simulate activity logs from a music streaming app based on specified configurations.
* **Project on Github**: [Sparkify](https://github.com/mrjaketomlinson/sparkify).
""")


# load_data helping functions
def process_song_file(filepath):
    """
    Description: This function is responsible for processing song_file data. The
    function creates a song table and artist table.
    
    Arguments:
        filepath: The song_file filepath to be processed.
        
    Returns:
        song_data: A dataframe of song data.
        artist_data: A dataframe of artist data.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # song_df
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]    
    # artist_df
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]

    return song_data, artist_data


def process_log_file(filepath, song_data, artist_data):
    """
    Description: This function is responsible for processing log_file data. The 
    function filters the data in the file, and adds records to the time, users,
    and songplays table.
    
    Arguments:
        filepath: The log_file filepath to be processed.
        song_data: The songs pandas dataframe.
        artist_data: the artists pandas dataframe.
    
    Returns:
        time_df: The time table extracted from this file.
        user_df: The user table extracted from this file.
        songplay_df: The songplay table extracted from this file, merged with
            the song and artist tables.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action and modify columns
    df = df[df['page'] == 'NextSong']
    df['ts_datetime'] = df['ts'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0))
    df['userId'] = df['userId'].apply(lambda x: int(x))

    # convert timestamp column to datetime
    t = df['ts_datetime']
    
    # time dataframe
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

    # user df
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset=['userId'], keep='first').reset_index(drop=True)
    
    # song_artist_df
    song_artist_df = song_data.merge(artist_data, how='left', left_on='artist_id', right_on='artist_id')

    # songplay_df
    songplay_df = pd.DataFrame()
    for index, row in df.iterrows():
        
        # get songid and artistid from song_artist_df
        ids = song_artist_df[(song_artist_df.title == row.song) & (song_artist_df.artist_name == row.artist) & (song_artist_df.duration == row.length)]

        # append songplay record
        songplay_data = {
            'timestamp': row.ts_datetime,
            'user_id': row.userId,
            'level': row.level,
            'song_id': str(ids.song_id) if ids.song_id.empty is False else 'None',
            'artist_id': str(ids.artist_id) if ids.artist_id.empty is False else 'None',
            'session_id': row.sessionId,
            'location': row.location,
            'user_agent': row.userAgent
        }
        songplay_df = songplay_df.append(songplay_data, ignore_index=True)
    
    return time_df, user_df, songplay_df


def get_files(filepath):
    """
    Description: This function gets the paths of all of the files in the 
    specified filepath.
    
    Arguments:
        filepath: A folder path to traverse.
    
    Returns:
        all_files: A list of files in the filepath.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files

        
# Grab data
@st.cache
def load_data():
    """
    Description: The function loads all of the data from the data files in the
    project by looping through the files and appending it to their respective
    dataframes.
    
    Arguments:
        None
    
    Returns:
        songs: A dataframe of songs.
        artists: A dataframe of artists.
        users: A dataframe of users.
        time: A dataframe of time.
        songplays: A dataframe of songplays.
    """
    # Instantialize data frames
    songs = pd.DataFrame()
    artists = pd.DataFrame()
    users = pd.DataFrame()
    time = pd.DataFrame()
    songplays = pd.DataFrame()
    
    # Get filepaths of data
    song_files = get_files('data/song_data')
    log_files = get_files('data/log_data')
    
    # Get song data
    for f in song_files:
        s, a = process_song_file(f)
        songs = pd.concat([songs, s]).reset_index(drop=True)
        artists = pd.concat([artists, a]).reset_index(drop=True)
    
    for f in log_files:
        t, u, sp = process_log_file(f, songs, artists)
        time = pd.concat([time, t]).reset_index(drop=True)
        users = pd.concat([users, u]).drop_duplicates(subset=['userId'], keep='first').reset_index(drop=True)
        songplays = pd.concat([songplays, sp]).reset_index(drop=True)
    
    return songs, artists, users, time, songplays


songs, artists, users, time, songplays = load_data()

# Column settings
col1, col2 = st.columns((2,1))

# col1
def add_labels(x, y):
    """
    Description: This function adds data labels to the active matplotlib figure.
    
    Arguments:
        x: The x axis values.
        y: The y axis values.

    Returns:
        None
    """
    for i in range(len(x)):
        plt.text(i, y[i], y[i], horizontalalignment='center')

## Top listeners
col1.subheader('Top 10 Listeners')
listeners = songplays.merge(users, how='left', left_on='user_id', right_on='userId')
listeners['Name'] = listeners['firstName'] + ' ' + listeners['lastName']
top_listeners = listeners.groupby(['Name'])['user_id'].count().reset_index(name='count').sort_values(['count'], ascending=False)
fig1 = plt.figure(figsize=(8,4))
plt.bar(top_listeners['Name'].values[:10], top_listeners['count'].values[:10])
plt.xticks(rotation=45, fontsize='small', horizontalalignment='right')
plt.xlabel('User')
plt.ylabel('Song plays')
add_labels(top_listeners['Name'].values[:10], top_listeners['count'].values[:10])
col1.pyplot(fig1)

## Song plays by level
col1.subheader('Song plays by level')
level_listeners = listeners.groupby(['level_x'])['user_id'].count().reset_index(name='count').sort_values(['count'], ascending=False)
fig2 = plt.figure(figsize=(8,4))
plt.bar(level_listeners['level_x'].values, level_listeners['count'].values)
plt.xticks(rotation=45, fontsize='small', horizontalalignment='right')
plt.xlabel('Level')
plt.ylabel('Song plays')
add_labels(level_listeners['level_x'].values, level_listeners['count'].values)
col1.pyplot(fig2)

## Song plays by gender
col1.subheader('Song plays by gender')
gender_listeners = listeners.groupby(['gender'])['user_id'].count().reset_index(name='count').sort_values(['count'], ascending=False)
gender_listeners['gender'] = gender_listeners['gender'].apply(lambda x: 'Female' if x == 'F' else 'Male')
fig3 = plt.figure(figsize=(8,4))
plt.bar(gender_listeners['gender'].values, gender_listeners['count'].values)
plt.xticks(rotation=45, fontsize='small', horizontalalignment='right')
plt.xlabel('Gender')
plt.ylabel('Song plays')
add_labels(gender_listeners['gender'].values, gender_listeners['count'].values)
col1.pyplot(fig3)

# col2
col2.subheader('Songs Table')
col2.dataframe(songs)

col2.subheader('Artists Table')
col2.dataframe(artists)

col2.subheader('Users Table')
col2.dataframe(users)

col2.subheader('Time Table')
col2.dataframe(time)

col2.subheader('Song Plays Table')
col2.dataframe(songplays)