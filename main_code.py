import os
import pandas as pd
from google.cloud import bigquery, storage

def xlsx_to_csv(event, context):
    """Cloud Function that converts two XLSX sheets to CSV and loads them to BigQuery."""
    
    # Get the name of the file that triggered the function
    file_name = 'task.xlsx'
    
    # Specify the GCS bucket name
    bucket_name = 'sarthaksrh'
    
    # Initialize the GCS client and get the XLSX file from the bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
        # List of sheet names to process
    sheet_names = ['Sheet1', 'Sheet2']

# Use pandas to read the XLSX file and convert each sheet to CSV
    sheets = pd.read_excel(blob.download_as_bytes(), sheet_name=sheet_names)

    for sheet_name, sheet_data in sheets.items():
    # Create CSV file name
        csv_name = os.path.splitext(file_name)[0] + '_' + sheet_name + '.csv'
    
    # Convert sheet data to CSV
        csv_data = sheet_data.to_csv(index=False)
    
    # Upload the CSV file to the GCS bucket
        csv_blob = bucket.blob(csv_name)
        csv_blob.upload_from_string(csv_data)
    
        print(f'File {csv_name} uploaded to {bucket_name}.')
    
    # Load the CSV data into a BigQuery table with the same name as the sheet
        bigquery_client = bigquery.Client()
        table_name = csv_name[:-4] # remove '.csv' from the end of the file name
    
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )
    
        uri = f'gs://{bucket_name}/{csv_name}'
        table_ref = bigquery_client.dataset('sarthak').table(table_name)
        load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
    
        print(f'Data loaded from {uri} to table {table_name}.')
    
    # Delete the CSV file
        csv_blob.delete()
    
        print(f'File {csv_name} deleted from {bucket_name}.')

    


        
    
          
