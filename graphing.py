import matplotlib.pyplot as plt
import pandas as pd
from hdfs import InsecureClient

client_hdfs = InsecureClient('http://localhost:9870')

name_of_files = ['h7', 'h15', 'h85', 'h96']
file_name_read = '/practice_3_h_files/'
file_name_read_stationlog = '/new_stationlog/stationlog.'

first_h = name_of_files.pop()
with client_hdfs.read(file_name_read + first_h +'.csv', encoding='utf-8') as File:
    with client_hdfs.read(file_name_read_stationlog + first_h + '.csv') as Stationlog:
        df = pd.read_csv(File)
        df_stationlog = pd.read_csv(Stationlog)
        print(df)
        print(df_stationlog)
        df = pd.merge(df, df_stationlog, on='\tTickTime', how='inner')
        print(df)
        ax = df.plot(x='\tError', y='\tSpeed', marker='o', label=first_h)


for i in name_of_files:
    with client_hdfs.read(file_name_read + i + '.csv', encoding='utf-8') as File:
        with client_hdfs.read(file_name_read_stationlog + i + '.csv') as Stationlog:
            df = pd.read_csv(File)
            df_stationlog = pd.read_csv(Stationlog)
            df = pd.merge(df, df_stationlog, on='\tTickTime', how='inner')
            df.plot(x='\tError', y='\tSpeed', marker='o', label=i, ax=ax)

plt.show()
plt.title('Скорости и ошибки в одни и те же моменты времени')
plt.xlabel('Скорости')
plt.ylabel('Ошибки')
plt.legend()
plt.savefig('result.png')
