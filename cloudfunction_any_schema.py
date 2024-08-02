import json, sys
import os
import csv
from google.cloud import storage, bigquery
from googleapiclient.discovery import build
from google.api_core.exceptions import GoogleAPIError, Conflict, NotFound

bucket_name2 = 'extra-files-bucket'

def process_csv(event,element):
    # Extract relevant information from the event
    print(event)
    bucket_name = event["bucket"]
    file_name = event["name"]

    try:
        # Initialize clients
        storage_client = storage.Client()
        bigquery_client = bigquery.Client()

        # Download the CSV file
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        local_path = f'/tmp/{file_name}'
        blob.download_to_filename(local_path)

        # Create a new CSV file without the first row
        modified_file_path = f'/tmp/modified_{file_name}'
        remove_first_row(local_path, modified_file_path)

        # Create BigQuery schema JSON
        schema_json_path = f'/tmp/schema.json'
        schema, fields = infer_schema(local_path)
        with open(schema_json_path, 'w') as schema_file:
            json.dump({"BigQuery Schema": schema}, schema_file)

        # Create JavaScript UDF file
        udf_js_path = f'/tmp/udf.js'
        create_js_udf(udf_js_path, fields)

        # Upload schema JSON, UDF, and modified CSV to bucket_name2
        extra_bucket = storage_client.bucket(bucket_name2)

        schema_blob = extra_bucket.blob(f'schema.json')
        schema_blob.upload_from_filename(schema_json_path)
        
        udf_blob = extra_bucket.blob('udf.js')
        udf_blob.upload_from_filename(udf_js_path)

        modified_blob = extra_bucket.blob(f'modified_{file_name}')
        modified_blob.upload_from_filename(modified_file_path)

        # Create BigQuery table based on inferred schema
        table_id = create_bigquery_table(bigquery_client, schema, file_name.split('.')[0])

        # Launch Dataflow job
        launch_dataflow_job(bucket_name2, f'modified_{file_name}', table_id, schema_blob.public_url, udf_blob.public_url)
        
    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('-----------------Error----------')
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)

def remove_first_row(original_path, modified_path):
    with open(original_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip the first row
        with open(modified_path, 'w', newline='') as csvfile_mod:
            writer = csv.writer(csvfile_mod)
            for row in reader:
                writer.writerow(row)

def infer_schema(file_path):
    schema = []
    fields = []
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for field in reader.fieldnames:
            schema.append({"name": field, "type": "STRING"})  # Infer as STRING, adjust as needed
            fields.append(field)
    return schema, fields

def create_bigquery_table(client, schema, table_name):
    try:
        dataset_id = 'TestData'  # Replace with your dataset ID
        project = 'norse-lens-429400-m1'
        table_id = f"{project}.{dataset_id}.{table_name}"
        table_output = f"{project}:{dataset_id}.{table_name}"

        print(f"Creating table with ID: {table_id}")

        # Adjust schema format for BigQuery
        schema_fields = [bigquery.SchemaField(field['name'], field['type']) for field in schema]
        table = bigquery.Table(table_id, schema=schema_fields)
        table = client.create_table(table)  # Attempt to create the table
        print(f"Created table {table_id}")
        return table_output
    except Conflict:
        print(f"Table {table_name} already exists. Proceeding with existing table.")
        return table_output
    except NotFound as e:
        print(f"Dataset {dataset_id} not found: {e}")
        raise
    except GoogleAPIError as e:
        print(f"Failed to create table {table_name}: {e}")
        raise

def create_js_udf(file_path, fields):
    field_mappings = ','.join([f'"{field}": val[{i}]' for i, field in enumerate(fields)])
    udf_code = f"""
function transform(inJson) {{
    val = inJson.split(",");
    const obj = {{ {field_mappings} }};
    return JSON.stringify(obj);
}}
"""
    with open(file_path, 'w') as udf_file:
        udf_file.write(udf_code)

def launch_dataflow_job(bucket_name, file_name, table_id, schema_gcs_path, udf_gcs_path):
    try:
        project = 'norse-lens-429400-m1'
        job_name = f"csv-to-bigquery-{file_name.replace('.', '-')}"
        template = 'gs://dataflow-templates/latest/GCS_Text_to_BigQuery'
        input_file = f"gs://{bucket_name}/{file_name}"
        temp_location = f"gs://{bucket_name}/dataflow-temp"

        parameters = {
            'inputFilePattern': input_file,
            'JSONPath': f"gs://{bucket_name2}/schema.json",  # Path to JSON schema file
            'outputTable': table_id,
            'javascriptTextTransformGcsPath': f"gs://{bucket_name2}/udf.js",  # Path to JavaScript UDF
            'javascriptTextTransformFunctionName': 'transform',  # Name of the UDF function
            'bigQueryLoadingTemporaryDirectory': temp_location,
        }

        dataflow = build('dataflow', 'v1b3')
        request = dataflow.projects().templates().launch(
            projectId=project,
            gcsPath=template,
            body={
                'jobName': job_name,
                'parameters': parameters,
                'environment': {
                    'tempLocation': temp_location,
                    'zone': 'us-central1-f'
                }
            }
        )

        response = request.execute()
        print(f"Dataflow job launched: {response}")

        # Print the detailed response for debugging
        print(f"Dataflow response: {json.dumps(response, indent=2)}")

    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('-----------------Error----------')
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)

