from googleapiclient.discovery import build


def trigger_df_job(cloud_event):   
 
    service = build('dataflow', 'v1b3')
    project = "norse-lens-429400-m1"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
    "jobName": "bq-load",  # Provide a unique name for the job
    "parameters": {
        "javascriptTextTransformGcsPath": "gs://extra-files-bucket/udf.js",
        "JSONPath": "gs://extra-files-bucket/BQSchema.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "norse-lens-429400-m1:TestData.DataBQ",
        "inputFilePattern": "gs://landing-files-bucket/dataset.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://extra-files-bucket"
    }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)



