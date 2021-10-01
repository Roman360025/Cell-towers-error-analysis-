from hdfs import InsecureClient
import pandas as pd
import sys
import csv

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


# def create_h_files(log_files):
#     file_name_read = '/new_files_3/'
#     first_file = log_files.pop()
#     name_of_h = first_file.split('.')[1]
#     with client_hdfs.read(file_name_read + first_file, encoding='utf-8') as File:
#         general_df = pd.read_csv(File)
#
#
#     # for i in log_files:
#     #     file_name_read = '/new_files_3/' + i
#     #     with client_hdfs.read(file_name_read, encoding='utf-8') as File1:
#     #         df = pd.read_csv(File1)
#     #         general_df = pd.concat([df, general_df], axis=0)
#
#     general_df = general_df.groupby(['Day', '\tTickTime']).sum()
#     new_name_of_file = '/practice_3_h_files/' + name_of_h + '.csv'
#     with client_hdfs.write(new_name_of_file, encoding='utf-8') as NewFile:
#         general_df.to_csv(NewFile)

def create_h_file(log_file):
    file_name_read = '/list_h_3/'
    with client_hdfs.read(file_name_read + log_file, encoding='utf-8') as File:
        general_df = pd.read_csv(File)


    general_df = general_df.groupby(['Day', '\tTickTime']).sum()
    new_name_of_file = '/practice_3_h_files/' + 'h7' + '.csv'
    with client_hdfs.write(new_name_of_file, encoding='utf-8') as NewFile:
        general_df.to_csv(NewFile)





list_of_files = []
general_df = None
file_name_read = '/new_files_3/'

for i in sys.stdin:
    i = i.rstrip()
    process_files(i)
    list_of_files.append(i)
    print(i)

# create_h_files(list_of_files)
