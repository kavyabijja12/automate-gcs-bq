# Automate data upload to Bigquery 

## Introduction 
create a cloud function trigger to automatically create table and upload data to Bigquery

## Architecture 
<img src="Architecture.png">

## Process
- Upload data to gcs bucket
<img src="images/Bigquery.png">
- cloud functon triggers
- creates Bigquery table with the required schema
<img src="images/cloudfunction.png">
- Trigger cloud function to upload data to bigquery
- Data is loaded to Bigquery
<img src="images/gcsbucket.png">

## Technology Used
- Programming Language - Python
- Google cloud platform
1. google cloud storage
2. Cloud function
3. Cloud run
4. Data flow
5. Bigquery


