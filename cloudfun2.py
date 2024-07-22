import google.auth
from googleapiclient.discovery import build

def run_dataflow(event, context):
    credentials, project = google.auth.default()
    dataflow = build('dataflow', 'v1b3', credentials=credentials)

    job = {
        'projectId': project,
        'jobName': 'gcs-to-bq-job',
        'gcsPath': 'gs://extra-files-bucket',
        'parameters': {
            'inputFile': f'gs://{event["bucket"]}/{event["name"]}',
            'outputTable': 'norse-lens-429400-m1:TestData.DataBQ'
        }
    }

    dataflow.projects().locations().templates().launch(
        projectId=project,
        location='us-central1',
        body=job
    ).execute()