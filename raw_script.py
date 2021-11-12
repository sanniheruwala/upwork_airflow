import sqlite3
import pandas as pd

### Create table ###

database = 'transfer'
conn = sqlite3.connect(database) 
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS filesData (
	source_file_path TEXT,
   	source_file_format TEXT,
	target_file_path TEXT,
	target_file_format TEXT,
	status TEXT) ''')

c.execute('''
    INSERT INTO filesData (source_file_path,source_file_format,target_file_path,target_file_format,status)
    VALUES
    ('data/inputData.parquet','parquet','output/jsonData.json','json','P'),
    ('data/inputData.parquet','parquet','output/csvData.csv','csv','P') ''')

conn.commit()
conn.close()

### update to S ###

conn = sqlite3.connect('transfer')
c = conn.cursor()
c.execute("update filesData set status='S' where status='P'")
conn.commit()
conn.close()

### read with S ###

conn = sqlite3.connect('transfer')
c = conn.cursor()
data = c.execute("SELECT * FROM filesData where status='S'").fetchall()
conn.commit()
conn.close()

### transform ###

for (source_file_path,source_file_format,target_file_path,target_file_format,status) in data:
	df = pd.read_parquet(source_file_path)
	if(target_file_format=='csv'):
		df.to_csv(target_file_path)
	if(target_file_format=='json'):
		df.to_json(target_file_path)

### update to C ###

conn = sqlite3.connect('transfer')
c = conn.cursor()
c.execute("update filesData set status='C' where status='S'")
conn.commit()
conn.close()