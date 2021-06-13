import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Takes in a filepath and cur to execute postgres commands on the song_file
    
    Arguments:
    cur = database cursor 
    filepath = file to the path to be opened and processed 
    '''
    # open song file
    df = pd.read_json(filepath, lines=True) 

    # insert song record
    song_data = [df.values[0][7], df.values[0][8], df.values[0][0], df.values[0][9], df.values[0][5]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.values[0][0], df.values[0][4], df.values[0][2], df.values[0][1], df.values[0][3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Takes in a filepath and cur to execute postgres commands on the log_file
    
    Arguments:
    cur = database cursor 
    filepath = file to the path to be opened and processed 
    '''
    # open log file
    df = df = pd.read_json(filepath, lines=True) 

    # filter by NextSong action
    df =df[df['page']=='NextSong']

    # convert timestamp column to datetime
    tf = df['ts']
    t = pd.Series(pd.to_datetime(tf, unit='ms'))
   
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    data = {column_labels[0]: time_data[0], column_labels[1]: time_data[1], column_labels[2]: time_data[2], column_labels[3]: time_data[3], column_labels[4]: time_data[4], column_labels[5]: time_data[5], column_labels[6]: time_data[6]}
    time_df = pd.DataFrame.from_dict(data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    users = [df['userId'], df['firstName'], df['lastName'], df['gender'], df['level']]
    column_labels = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    data = {column_labels[0]: users[0], column_labels[1]: users[1], column_labels[2]: users[2], column_labels[3]: users[3], column_labels[4]: users[4]}
    user_df = pd.DataFrame.from_dict(data)


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
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Finds and processes files
    
    Arguments:
    cur = database cursor 
    conn = connection to the database 
    filepath = file to the path to be opened and processed 
    func = function used to call each file
    '''
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()