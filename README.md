# Upwork Airflow Task

This airflow DAG is responsible for 
conversion of data from parquet to JSON and CSV.

Flow requires to read data from sqlite
and take out all the pending status records.

Later it reads those files in pandas and 
convert them as per mentioned target format.

In between to handle exceptions it stores the status.
P -> S -> C
P = Pending
S = Started transforamation
C = Transformation complete