import io
import json
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- Configurations ---
SECRET_NAME = "googledrive/api/credentials"
REGION_NAME = "us-east-1"  # Replace with your AWS Region
S3_BUCKET = "glueproject-bucket-871049984307-us-east-1-an"
FOLDER_ID = "1yIiTpsSnRiebJzVLe00ll75_7WIHJUkV"  # The ID of the folder containing the shortcuts


def get_google_credentials():
    """Retrieves JSON credentials from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response['SecretString'])

def download_direct_csv_files():
    # 1. Authenticate with Google
    creds_dict = get_google_credentials()
    creds = service_account.Credentials.from_service_account_info(creds_dict)
    drive_service = build('drive', 'v3', credentials=creds)
    s3_client = boto3.client('s3')
    
    print(f"Scanning folder: {FOLDER_ID} for direct table files...")
    
    # Query targets files directly inside your folder, excluding trashed files
    query = f"'{FOLDER_ID}' in parents and trashed = false"
    
    results = drive_service.files().list(
        q=query,
        fields="nextPageToken, files(id, name, mimeType)",
        pageSize=1000,
        supportsAllDrives=True,           # ◄ FORCES VISIBILITY ON INDIVIDUAL SHARED ASSETS
        includeItemsFromAllDrives=True    # ◄ MAKES DIRECT USER DRIVE UPLOADS DISCOVERABLE
    ).execute()

    
    files = results.get('files', [])
    if not files:
        print("No files found in this folder. Double check folder ID and permissions.")
        return

    print(f"Found {len(files)} total items in folder. Filtering for tables...")

    # 2. Loop through the actual files
    for item in files:
        file_id = item['id']
        file_name = item['name']
        mime_type = item['mimeType']
        
        # Skip folders if any exist in this directory
        if mime_type == 'application/vnd.google-apps.folder':
            print(f"Skipping nested folder: '{file_name}'")
            continue

        # Check if the file is a native Google Sheet or a standard flat CSV file
        is_sheet = (mime_type == "application/vnd.google-apps.spreadsheet")
        is_csv_file = (mime_type == "text/csv" or file_name.lower().endswith('.csv'))
        
        if not (is_sheet or is_csv_file):
            print(f"Skipping '{file_name}' - Not a CSV or Google Sheet (Type: {mime_type}).")
            continue
            
        print(f"Processing target file: '{file_name}' (ID: {file_id})")
        
        # 3. Stream data into memory and upload to S3
        file_stream = io.BytesIO()
        try:
            if is_sheet:
                # Convert native Google Sheets to raw CSV format on the fly
                request = drive_service.files().export_media(fileId=file_id, mimeType='text/csv')
                if not file_name.lower().endswith('.csv'):
                    file_name = f"{file_name}.csv"
            else:
                # Download standard uploaded CSV files directly
                request = drive_service.files().get_media(fileId=file_id)
                
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                
            # Upload file contents directly to your S3 folder path
            file_stream.seek(0)
            s3_key = f"data/{file_name}"
            
            print(f"Uploading file to s3://{S3_BUCKET}/{s3_key}")
            s3_client.upload_fileobj(file_stream, S3_BUCKET, s3_key)
            print(f"✅ Successfully uploaded '{file_name}'")
            
        except Exception as e:
            print(f"⚠️ Error downloading/uploading '{file_name}': {str(e)}")
            continue

    print("Pipeline execution successfully complete!")

if __name__ == "__main__":
    download_direct_csv_files()
