from hdfs import InsecureClient
import pandas as pd
import sys

client_hdfs = InsecureClient('http://localhost:9870')


def process_files(log_file):
    file_name_read = '/list_h_3/' + log_file
    file_name_write = '/new_files_3/' + log_file
    with client_hdfs.read(file_name_read, encoding='utf-8') as File:
        with client_hdfs.write(file_name_write, encoding='utf-8') as NewFile:
            df = pd.read_csv(File)
            df.drop(df.index[(df['Day'] != 7)], axis=0, inplace=True)
            df['\tTickTime'] = df['\tTickTime'] / 300
            df['\tTickTime'] = df['\tTickTime'].astype(int)
            df = df.groupby(['Day', '\tTickTime'])['\tSpeed'].sum()
            df.to_csv(NewFile)

    return df

def process_station_log(log_file):
    if 'stationlog' in log_file:
        file_name_read = '/stationlog/' + log_file
        file_name_write = '/new_stationlog/' + log_file
    else:
        file_name_read = '/list_h_3/' + log_file
        file_name_write = '/new_files_3/' + log_file
    with client_hdfs.read(file_name_read, encoding='utf-8') as File:
        with client_hdfs.write(file_name_write, encoding='utf-8') as NewFile:
            df = pd.read_csv(File)
            if 'list_h_3' in file_name_read:
                df.drop(df.index[(df['Day'] != 7)], axis=0, inplace=True)
            df['\tTickTime'] = df['\tTickTime'] / 300
            df['\tTickTime'] = df['\tTickTime'].astype(int)
            if 'list_h_3' in file_name_read:
                df = df.groupby(['Day', '\tTickTime'])['\tSpeed'].sum()
            else:
                df = df.groupby(['Day', '\tTickTime'])['\tError'].sum()
            df.to_csv(NewFile)








for i in sys.stdin:
    i = i.rstrip()
    process_station_log(i)
    print(i)

