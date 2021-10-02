import pandas as pd
import sys
from hdfs import InsecureClient

client_hdfs = InsecureClient('http://localhost:9870')

general_df = None
name_of_h = None

for i in sys.stdin:
    i = i.rstrip()
    if name_of_h is None:
        name_of_h = i.split('.')[1]
    if 'userlog' in i:
        file_name_read = '/new_files_3/' + i
    else:
        continue
    with client_hdfs.read(file_name_read, encoding='utf-8') as File:
        df = pd.read_csv(File)
        if general_df is not None:
            general_df = pd.concat([df, general_df], axis=0)
        else:
            general_df = df

general_df = general_df.groupby(['Day', '\tTickTime']).sum()

new_name_of_file = '/practice_3_h_files/' + name_of_h + '.csv'
with client_hdfs.write(new_name_of_file, encoding='utf-8') as NewFile:
    general_df.to_csv(NewFile)
