import io
import boto3
import pandas as pd

# --- Updated Configurations ---
BUCKET_NAME = "glueproject-bucket-871049984307-us-east-1-an"            # The unified S3 Bucket for your project
SOURCE_PREFIX = "data"                                                  # Landing / Ingestion Folder
BRONZE_PREFIX = "bronze"                                                # Cleaned / Treated Bronze Folder

# --- Column Selections (Specify exactly what you want to keep) ---
# Replace these placeholder lists with your actual column names
PROVIDER_INFO_COLS = [
"CMS Certification Number (CCN)",
"Provider Name",
"Provider Address",
"City/Town",
"State",
"ZIP Code",
"Telephone Number",
"Provider SSA County Code",
"County/Parish",
"Ownership Type",
"Number of Certified Beds",
"Average Number of Residents per Day",
"Provider Type",
"Provider Resides in Hospital",
"Legal Business Name",
"Date First Approved to Provide Medicare and Medicaid Services",
"Affiliated Entity Name",
"Affiliated Entity ID",
"Abuse Icon",
"Overall Rating",
"Health Inspection Rating",
"QM Rating",
"Long-Stay QM Rating",
"Short-Stay QM Rating",
"Staffing Rating",
"Staffing Rating Footnote",
"Reported Staffing Footnote",
"Physical Therapist Staffing Footnote",
"Reported Nurse Aide Staffing Hours per Resident per Day",
"Reported LPN Staffing Hours per Resident per Day",
"Reported RN Staffing Hours per Resident per Day",
"Reported Licensed Staffing Hours per Resident per Day",
"Reported Total Nurse Staffing Hours per Resident per Day",
"Total number of nurse staff hours per resident per day on the weekend",
"Registered Nurse hours per resident per day on the weekend",
"Reported Physical Therapist Staffing Hours per Resident Per Day",
"Total nursing staff turnover",
"Total nursing staff turnover footnote",
"Registered Nurse turnover",
"Registered Nurse turnover footnote",
"Number of administrators who have left the nursing home",
"Administrator turnover footnote",
"Nursing Case-Mix Index",
"Nursing Case-Mix Index Ratio",
"Case-Mix Nurse Aide Staffing Hours per Resident per Day",
"Case-Mix LPN Staffing Hours per Resident per Day",
"Case-Mix RN Staffing Hours per Resident per Day",
"Case-Mix Total Nurse Staffing Hours per Resident per Day",
"Case-Mix Weekend Total Nurse Staffing Hours per Resident per Day",
"Adjusted Nurse Aide Staffing Hours per Resident per Day",
"Adjusted LPN Staffing Hours per Resident per Day",
"Adjusted RN Staffing Hours per Resident per Day",
"Adjusted Total Nurse Staffing Hours per Resident per Day",
"Adjusted Weekend Total Nurse Staffing Hours per Resident per Day",
"Total Weighted Health Survey Score",
"Number of Facility Reported Incidents",
"Number of Substantiated Complaints",
"Number of Citations from Infection Control Inspections",
"Number of Fines",
"Total Amount of Fines in Dollars",
"Number of Payment Denials",
"Total Number of Penalties",
"Processing Date" 
    ]
STATE_AVG_COLS = [
"State or Nation",
"Average Number of Residents per Day",
"Reported Nurse Aide Staffing Hours per Resident per Day",
"Reported LPN Staffing Hours per Resident per Day",
"Reported RN Staffing Hours per Resident per Day",
"Reported Licensed Staffing Hours per Resident per Day",
"Reported Total Nurse Staffing Hours per Resident per Day",
"Total number of nurse staff hours per resident per day on the weekend",
"Registered Nurse hours per resident per day on the weekend",
"Reported Physical Therapist Staffing Hours per Resident Per Day",
"Total nursing staff turnover",
"Registered Nurse turnover",
"Number of administrators who have left the nursing home",
"Nursing Case-Mix Index",
"Case-Mix RN Staffing Hours per Resident per Day",
"Case-Mix Total Nurse Staffing Hours per Resident per Day",
"Case-Mix Weekend Total Nurse Staffing Hours per Resident per Day",
"Number of Fines",
"Fine Amount in Dollars",
"Percentage of long stay residents whose need for help with daily activities has increased",
"Percentage of long stay residents who lose too much weight",
"Percentage of low risk long stay residents who lose control of their bowels or bladder",
"Percentage of long stay residents with a catheter inserted and left in their bladder",
"Percentage of long stay residents with a urinary tract infection",
"Percentage of long stay residents who have depressive symptoms",
"Percentage of long stay residents who were physically restrained",
"Percentage of long stay residents experiencing one or more falls with major injury",
"Percentage of long stay residents assessed and appropriately given the pneumococcal vaccine",
"Percentage of long stay residents who received an antipsychotic medication",
"Percentage of short stay residents assessed and appropriately given the pneumococcal vaccine",
"Percentage of short stay residents who newly received an antipsychotic medication",
"Percentage of long stay residents whose ability to move independently worsened",
"Percentage of long stay residents who received an antianxiety or hypnotic medication",
"Percentage of high risk long stay residents with pressure ulcers",
"Percentage of long stay residents assessed and appropriately given the seasonal influenza vaccine",
"Percentage of short stay residents who made improvements in function",
"Percentage of short stay residents who were assessed and appropriately given the seasonal influenza vaccine",
"Percentage of short stay residents who were rehospitalized after a nursing home admission",
"Percentage of short stay residents who had an outpatient emergency department visit",
"Number of hospitalizations per 1000 long-stay resident days",
"Number of outpatient emergency department visits per 1000 long-stay resident days",
"Processing Date"
    ]

def get_s3_client():
    return boto3.client('s3')

def read_csv_from_s3(s3_client, bucket, key, dtype_dict=None):
    """Downloads an S3 object straight into a Pandas DataFrame stream."""
    print(f"Reading file: s3://{bucket}/{key}")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    # Using low_memory=False prevents mixed data type inference warnings
    # Adding encoding='latin-1' bypasses UnicodeDecodeErrors for special characters
    return pd.read_csv(
        io.BytesIO(obj['Body'].read()), 
        dtype=dtype_dict, 
        low_memory=False, 
        encoding="latin-1"
    )

def write_df_to_s3(s3_client, df, bucket, key):
    """Streams a processed Pandas DataFrame directly back up to S3."""
    print(f"Writing file to s3://{bucket}/{key}")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def clean_and_transform_bronze():
    s3_client = get_s3_client()
    
    # ----------------------------------------------------
    # 1. Process File: NH_ProviderInfo_Oct2024.csv (9 MB)
    # ----------------------------------------------------
    # We force the primary key column to be read as a string so we can manipulate it safely
    ccn_col = "CMS Certification Number (CCN)"
    df_provider = read_csv_from_s3(
        s3_client, 
        BUCKET_NAME, 
        f"{SOURCE_PREFIX}/NH_ProviderInfo_Oct2024.csv",
        dtype_dict={ccn_col: str}
    )
    
    # Filter columns to only retain your specific schema
    df_provider = df_provider[PROVIDER_INFO_COLS]
    
    # Treat the primary key: Strip leading zeros and clear whitespace
    df_provider[ccn_col] = df_provider[ccn_col].str.strip().str.lstrip('0')
    
    # Standard Bronze metadata tracking: Add ingestion timestamp
    df_provider['bronze_ingested_at'] = pd.to_datetime('now')
    write_df_to_s3(s3_client, df_provider, BUCKET_NAME, f"{BRONZE_PREFIX}/NH_ProviderInfo_Oct2024_Cleaned.csv")
    
    # ----------------------------------------------------
    # 2. Process File: NH_StateUSAverages_Oct2024.csv (22 KB)
    # ----------------------------------------------------
    df_state = read_csv_from_s3(
        s3_client, 
        BUCKET_NAME, 
        f"{SOURCE_PREFIX}/NH_StateUSAverages_Oct2024.csv"
    )
    
    # Filter columns to only retain your specific schema
    df_state = df_state[STATE_AVG_COLS]
    
    # Standardize join key: Clean up formatting or trailing spaces in State column
    df_state['State or Nation'] = df_state['State or Nation'].astype(str).str.strip().str.upper()
    
    df_state['bronze_ingested_at'] = pd.to_datetime('now')
    write_df_to_s3(s3_client, df_state, BUCKET_NAME, f"{BRONZE_PREFIX}/NH_StateUSAverages_Oct2024_Cleaned.csv")

    # ----------------------------------------------------
    # 3. Process File: PBJ_Daily_Nurse_Staffing_Q2_2024.csv (210 MB)
    # ----------------------------------------------------
    # We read this large file last to keep memory clean during preceding runs
    df_staffing = read_csv_from_s3(
        s3_client, 
        BUCKET_NAME, 
        f"{SOURCE_PREFIX}/PBJ_Daily_Nurse_Staffing_Q2_2024.csv",
        dtype_dict={"PROVNUM": str}
    )
    
    # Retain all columns as requested. Clean up primary key whitespace.
    df_staffing['PROVNUM'] = df_staffing['PROVNUM'].astype(str).str.strip()
    
    df_staffing['bronze_ingested_at'] = pd.to_datetime('now')
    write_df_to_s3(s3_client, df_staffing, BUCKET_NAME, f"{BRONZE_PREFIX}/PBJ_Daily_Nurse_Staffing_Q2_2024_Cleaned.csv")

    print("🚀 All 3 files cleaned, columns stripped, keys treated, and moved to Bronze folder successfully!")

if __name__ == "__main__":
    clean_and_transform_bronze()
