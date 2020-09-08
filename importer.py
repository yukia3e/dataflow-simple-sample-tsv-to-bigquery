#!/usr/bin/env python3

import apache_beam as beam
import csv

def get_filename_newest_file(bucket_name, file_type, tdate, delimiter=None):
    from google.cloud import storage
    storage_client = storage.Client()

    # GCS Directory prefix
    prefixCondition = file_type + "/" + file_type + "_" + tdate + "_"
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefixCondition, delimiter=delimiter
    )
    from datetime import datetime as dt
    filename_list = {}
    for blob in blobs:
        file_date_str = blob.name.split('/')[-1].split('_')[-1].split('.')[0]
        tdatetime = dt.strptime(file_date_str, '%Y%m%d%H%M%S')
        filename_list[blob.name] = tdatetime
    maxResult = max(filename_list, key=filename_list.get)
    print(maxResult)
    return maxResult

def is_contents(fields, mark):
    return fields[0] != mark

def replace_double_quotes_in_line(line):
    return line.replace('"', '')

def str_to_date(fields, tdate):
    outputFields = []
    outputFields.append(tdate)
    for field in fields:
        outputFields.append(field)
    return outputFields

def create_row(header, fields):
    featdict = {}
    for name, value in zip(header, fields):
        featdict[name] = value
    return featdict

def run(project, dataset, storagebucket, workbucket, tdate):
    argv = [
        # xxxxxxx for DataflowRunner xxxxxxx
        '--runner=DataflowRunner',
        '--job_name=importer{0}'.format(tdate),
        '--project={0}'.format(project),
        '--staging_location=gs://{0}/dataflow/staging/'.format(workbucket),
        '--temp_location=gs://{0}/dataflow/temp/'.format(workbucket),
        '--region=asia-northeast1',
        '--save_main_session',
        '--max_num_workers=4',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--setup_file=./setup.py',
        # xxxxxxx for DataflowRunner xxxxxxx
        # ===== for DirectRunner ==========
        # '--runner=DirectRunner',
        # ===== for DirectRunner ==========
    ]

    with beam.Pipeline(argv=argv) as pipeline:
        # ---------------------------------------------------------------------------------------------------------------
        # ------------- 1. test_file start ----------------
        test_file_path = 'gs://{}/'.format(storagebucket) + get_filename_newest_file(storagebucket, 'test_file', tdate)

        ## header line pattern
        mark_test_file = "id"

        # CSV header line
        header_test_file = 'tdate,id,name,account_url,gender'.split(',')
        output_test_file = '{}:{}.test_file'.format(project, dataset)
        
        # BigQuery schema
        schema_test_file = 'tdate:date,id:string,name:string,account_url:string,gender:string'
        
        test_file = (pipeline
            | 'test_file:read' >> beam.io.ReadFromText(test_file_path)
            | 'test_file:escape' >> beam.Map(replace_double_quotes_in_line)
            | 'test_file:fields' >> beam.Map(lambda line: next(csv.reader([line], delimiter="\t")))
            | 'test_file:filter_header' >> beam.Filter(is_contents, mark=mark_test_file)
            | 'test_file:add_tdate' >> beam.Map(str_to_date, tdate='{0}-{1}-{2}'.format(tdate[0:4], tdate[4:6], tdate[6:8]))
            | 'test_file:totablerow' >> beam.Map(lambda fields: create_row(header_test_file, fields))
        )
        errors_test_file = (test_file
            | 'test_file:out' >> beam.io.WriteToBigQuery(
                output_test_file,
                schema=schema_test_file,
                insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        # ===== for DirectRunner ==========
        # result_test_file = (errors_test_file
        #     # | 'test_file:PrintErrors' >> beam.FlatMap(lambda err: print("[test_file]Error Found {}".format(err)))
        #     | 'test_file_error:PrintErrors' >> beam.io.WriteToText('test_file_error')
        # )
        # ===== for DirectRunner ==========
        
        # ------------- 1. test_file end ----------------
        # -------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument(
        '-p',
        '--project',
        help='Unique project ID',
        required=True
    )
    parser.add_argument(
        '-s', '--storagebucket',
        help='Google Cloud Storage Data Bucket Name',
        required=True
    )
    parser.add_argument(
        '-w', '--workbucket',
        help='Google Cloud Storage Work(tmp) Bucket Name',
        required=True
    )
    parser.add_argument(
        '-t',
        '--tdate',
        help='TargetDate（ex. 20200710）',
        required=True
    )
    parser.add_argument(
        '-d',
        '--dataset',
        help='BigQuery dataset',
        default='default_dataset'
    )
    args = vars(parser.parse_args())

    run(project=args['project'], dataset=args['dataset'], storagebucket=args['storagebucket'], workbucket=args['workbucket'], tdate=args['tdate'])