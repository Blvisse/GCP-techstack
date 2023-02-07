
'''
This python script uploads locally stored csv data into google bigquery 

'''

try:
    import pandas as pd
    import os
    from google.cloud import bigquery
    from pathlib import Path
    import time
    print("Successfully imported libraries")

except ImportError as ie:
    print ("Couldn't import {}'".format(ie.name))
    print("Run pip install {} ".format(ie.name))


#variables
project_id = 'dtc-dataeng2'
dataset_id=   'staging' 

# initialize python BigqueryAPI

client=bigquery.Client()
data_file_path=Path('../data')


def table_reference(project_id: str, dataset_id: str, table_id: str):
    dataset_ref=bigquery.DatasetReference(project_id, dataset_id)
    table_ref=bigquery.TableReference(dataset_ref, table_id)
    
    return table_ref

def upload_csv(client,table_ref,csv_file):
    load_job_configuration=bigquery.LoadJobConfig()
    
    #read_csv file
    
    #define table schema:
    # load_job_configuration.schema = [
    #     bigquery.SchemaField('Country_Name','STRING', mode='REQUIRED'),
    #     bigquery.SchemaField('Country_Code', 'STRING', mode='REQUIRED'),
    #     bigquery.SchemaField('Indicator_Name', 'STRING', mode='REQUIRED'),
    #     bigquery.SchemaField('Indicator_Code', 'STRING', mode='REQUIRED'),
        
    #     for field in data.columns[3:]:
    #         bigquery.SchemaField(field,'INTEGER',mode='NULLABLE'),
    # ]
    #other configurations
    load_job_configuration.autodetect=True
    load_job_configuration.source_format=bigquery.SourceFormat.CSV
    load_job_configuration.skip_leading_rows=1
    
    #batch load the data into bigquery using API request
    with open(csv_file, "rb") as source_file:
        upload_job=client.load_table_from_file(source_file,
                                               destination=table_ref,
                                               location="US",
                                               job_config=load_job_configuration,
                                               )
    
    while upload_job.state != 'DONE':
        time.sleep(2)
        upload_job.reload()
        print(upload_job.state)
    print(upload_job.result( ))
        
#loop through the data folder and store them in the bigquery

for file in os.listdir(data_file_path):
    #sanity check to ensure we only upload csv files
    if file.endswith('.csv'):
        print("Processing file {}".format(file))
        
        table_name=''.join(file.split(".")[0])
        csv_file=data_file_path.joinpath(file)
        # print(table_name)
        # print(csv_file)
        data=pd.read_csv(csv_file)
        print(data.columns[4])
        table_ref=table_reference(project_id,dataset_id,table_name)
        upload_csv(client,table_ref,csv_file)
        print("Done processing")



