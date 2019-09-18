import pandas as pd 
import os, glob
import datetime

import itertools
import threading
import time
import sys

# Set up BigQuery client
from google.cloud import bigquery
client = bigquery.Client()
filename = 'result.csv'
dataset_id = 'gsheets'
table_id = 'xb_shipped_report'

# Set up destination table and job configuration
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job_config.schema = [
    bigquery.SchemaField('header_ref','STRING'),
    bigquery.SchemaField('doc_date','DATE'),
    bigquery.SchemaField('item','STRING'),
    bigquery.SchemaField('eorder_doc_line_no','NUMERIC'),
    bigquery.SchemaField('quantity_ordered','NUMERIC'),
    bigquery.SchemaField('quantity_shipped','NUMERIC'),
    bigquery.SchemaField('unit_dimension','STRING'),
    bigquery.SchemaField('description','STRING'),
    bigquery.SchemaField('product_content','STRING'),
    bigquery.SchemaField('eorder_doc_no','STRING'),
    bigquery.SchemaField('source_file','STRING'),
    bigquery.SchemaField('source_sheet','STRING'),
    bigquery.SchemaField('processed_at','TIMESTAMP'),
]

# Set up local file processing
os.chdir('/Users/kevinkong/Documents/sandbox/misc/inventory-recon')
final_df = pd.DataFrame()
obj = ''

# Shortcut option
print('')
print('1. Full run.')
print('2. Import to BigQuery only.')
option = input('Choose a run option: ')

if option == '1':
    def animate():
        global obj
        global file
        global sheet
        max_len = 50
        if (obj == file):
            processing_string = 'Working on '
        elif (obj == sheet):
            processing_string = 'Parsing '
        else:
            processing_string = 'Packing into CSV'
            obj = ''
        
        sys.stdout.write('\r' + processing_string + obj)
        for i in range(1,max_len-len(processing_string)-len(obj)):
            sys.stdout.write('.')
        sys.stdout.write(' ')
        for c in itertools.cycle(['|', '/', '-', '\\']):
            if done:
                break
            sys.stdout.write('\b'+c)
            sys.stdout.flush()
            time.sleep(0.1)
        sys.stdout.write('\r' + processing_string + obj)
        for i in range(1,max_len-len(processing_string)-len(obj)):
            sys.stdout.write('.')
        sys.stdout.write('Done!\n')

    for file in glob.glob("*.xlsx"):
        done = False
        obj = file
        t = threading.Thread(target=animate)
        t.start()
        xls = pd.ExcelFile(file)
        done = True
        time.sleep(0.3)
        for sheet in xls.sheet_names:
            done = False
            obj = sheet
            t = threading.Thread(target=animate)
            t.start()
            temp_df = xls.parse(sheet)
            temp_df["source_file"] = file
            temp_df["source_sheet"] = sheet
            temp_df["processed_at"] = (datetime.datetime.now())
            final_df = final_df.append(temp_df, sort=False)
            done = True
            time.sleep(0.2)

    done = False
    obj = ''
    t = threading.Thread(target=animate)
    t.start()
    final_df.to_csv('result.csv',index=False)
    done = True
    time.sleep(0.2)

    print("Loading data into BigQuery...")
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="US",  # Must match the destination dataset location.
            job_config=job_config,
        )  # API request

elif option == '2':
    print("Loading data into BigQuery...")
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="US",  # Must match the destination dataset location.
            job_config=job_config,
        )  # API request

job.result()  # Waits for table load to complete.
print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))