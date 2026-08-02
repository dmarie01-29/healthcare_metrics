import io
import json
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- Configurations ---
SECRET_NAME = "google/drive/credentials"
REGION_NAME = "us-east-1"  # Replace with your AWS Region
S3_BUCKET = "your-target-s3-bucket"
SHORTCUT_ID = "YOUR_GOOGLE_DRIVE_SHORTCUT_FILE_ID" # The ID from the shortcut's URL

def get_google_credentials():
    """Retrieves JSON credentials from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response['SecretString'])

def download_shortcut_table():
    # 1. Authenticate with Google
    creds_dict = get_google_credentials()
    creds = service_account.Credentials.from_service_account_info(creds_dict)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # 2. Get shortcut metadata and find the actual underlying Target ID
    print(f"Fetching metadata for shortcut: {SHORTCUT_ID}")
    file_metadata = drive_service.files().get(
        fileId=SHORTCUT_ID, 
        fields="id, name, mimeType, shortcutDetails"
    ).execute()
    
    # Check if it is a true shortcut and resolve it
    if 'shortcutDetails' in file_metadata:
        target_id = file_metadata['shortcutDetails']['targetId']
        target_mime = file_metadata['shortcutDetails']['targetMimeType']
        filename = file_metadata['name']
        print(f"Resolved shortcut to target ID: {target_id} ({target_mime})")
    else:
        target_id = SHORTCUT_ID
        target_mime = file_metadata.get('mimeType')
        filename = file_metadata['name']
    
    # 3. Stream file contents into memory
    file_stream = io.BytesIO()
    
    # If the target is a native Google Sheet table, export it to CSV format
    if target_mime == "application/vnd.google-apps.spreadsheet":
        request = drive_service.files().export_media(fileId=target_id, mimeType='text/csv')
        filename = f"{filename}.csv" if not filename.endswith('.csv') else filename
    else:
        # Standard table file types (.csv, .xlsx) can be downloaded directly
        request = drive_service.files().get_media(fileId=target_id)
        
    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        
    # 4. Stream directly up into Amazon S3
    file_stream.seek(0)
    s3_client = boto3.client('s3')
    s3_key = f"downloaded_tables/{filename}"
    
    print(f"Uploading resolved table to s3://{S3_BUCKET}/{s3_key}")
    s3_client.upload_fileobj(file_stream, S3_BUCKET, s3_key)
    print("Download and migration successfully complete!")

if __name__ == "__main__":
    download_shortcut_table()
