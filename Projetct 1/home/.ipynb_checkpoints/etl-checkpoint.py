import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from io import StringIO 


def process_song_file(cur, filepath):
    """
    - Read and process the song files
    - Insert records into the song table
    - Insert records into the artists table
    """

    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id',
                    'year','duration'
                   ]].iloc[0].values.tolist()
    cur.execute(song_table_insert, ([song_data[0],song_data[1],song_data[2]
                                    ,song_data[3].item(),song_data[4].item()]))
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location',
                       'artist_latitude','artist_longitude'
                    ]].iloc[0].values.tolist() 
    cur.execute(artist_table_insert, ([artist_data[0],artist_data[1],
                                      artist_data[2],artist_data[3].item(),
                                      artist_data[4].item()]))

def copy_to_postgres(cur, df, table, columns):

    """
    - Bulk insert records for tables: songplays and time
    """

    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, sep="\t", index=False, header=False)
    buffer.seek(0)
    
    # Insert records
    try:
        cur.copy_from(buffer, table, columns=columns, sep = "\t", null = '')
        print
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    print("copy_to_postgres() done {}".format(table))
    

def process_log_file(cur, filepath):

    """
    - Process JSON logs files
    - Insert records into time table
    - Insert records into users table
    - Insert records into songplays table

    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query('page=="NextSong"')

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit ='ms')
    
    # insert time data records
    time_data = (t.dt.time,t.dt.hour,t.dt.day,t.dt.week,
                 t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 
                     'month', 'year', 'weekday')
    timeData = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame.from_dict(timeData)
    time_columns = ('star_time','hour','day','week','month','year','weekday')
    copy_to_postgres(cur,time_df,"time",time_columns)    
        

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # Insert user records 
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    

    # insert songplay records
    songplay_columns = ('start_time','user_id','level','song_id','artist_id',
                        'session_id','location','user_agent')
    songplay_data = []
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_dic =dict(zip(songplay_columns,(row.ts,row.userId,row.level,
                               songid,artistid,row.sessionId,row.location,
                               row.userAgent)))
        songplay_data.append(songplay_dic)
    
    # Bulk insert songplay records
    songplay_df = pd.DataFrame(songplay_data)
    songplay_df = songplay_df.reindex(columns=songplay_columns)
    copy_to_postgres(cur,songplay_df,"songplays",songplay_columns) 


def process_data(cur, conn, filepath, func):

    """
    - Load all JSON files in the direcories data/song_data and data/log_data.
    - Iterates over the files and calls the functions to process 
      the song and log files.
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
    - Open the connection to sparkifydb
    - Process song and log files
    - Load the data into the sparkifydb tables
    - Close the connection 
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.autocommit = True
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()