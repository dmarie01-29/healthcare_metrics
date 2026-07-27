import streamlit as st
import pandas as pd
import boto3

# 1. Page Configuration Settings
st.set_page_config(page_title="Healthcare Analytics Dashboard", layout="wide")
st.title("🏥 Universal Facility Staffing & Performance Visual Analytics")
st.markdown("---")

BUCKET_NAME = "glueproject-bucket-871049984307-us-east-1-an"
FILE_KEY = "gold/" 

# 2. Caching Function to Pull Metrics from S3
@st.cache_data(ttl=3600)
def load_data_from_s3(bucket, prefix):
    
    # ◄ REPLACE STARTING HERE: This robust block handles both local and cloud credentials
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
            region_name=st.secrets["aws"]["aws_default_region"]
        )
    except:
        s3_client = boto3.client('s3')
    # ◄ END OF REPLACEMENT
        
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = response.get('Contents', [])
    
    # Extract filename keys and their LastModified timestamps
    valid_files = [
        (f['Key'], f['LastModified']) 
        for f in files 
        if 'part-r-' in f['Key'] and f['Size'] > 0
    ]
    
    if not valid_files:
        raise FileNotFoundError(f"Could not locate active data partition blocks in s3://{bucket}/{prefix}")

        
    # Sort files by timestamp and grab only the newest one
    valid_files.sort(key=lambda x: x[1], reverse=True)
    newest_file_key = valid_files[0][0]
    
    file_obj = s3_client.get_object(Bucket=bucket, Key=newest_file_key)
    df = pd.read_csv(file_obj['Body'])
    
    # Standardize data types for sorting/Benjamin aggregation robustness
    df['PROVNUM'] = df['PROVNUM'].astype(str).str.strip()
    df['PROVNAME'] = df['PROVNAME'].astype(str).str.strip()
    df['STATE'] = df['STATE'].astype(str).str.upper().str.strip()
    
    # --- UPDATED TO MATCH YOUR EXACT VARIABLE FORMATS ---
    df['Tot_Nurse_hrs'] = pd.to_numeric(df['Tot_Nurse_hrs'], errors='coerce')
    df['MDScensus'] = pd.to_numeric(df['MDScensus'], errors='coerce')
    df['occup_rate'] = pd.to_numeric(df['occup_rate'], errors='coerce')
    df['overall_rating'] = pd.to_numeric(df['overall_rating'], errors='coerce')
    df['perm_staff'] = pd.to_numeric(df['perm_staff'], errors='coerce')
    df['temp_staff'] = pd.to_numeric(df['temp_staff'], errors='coerce')
    
    return df

try:
    df = load_data_from_s3(BUCKET_NAME, FILE_KEY)
    
    # ----------------------------------------------------
    # 3. SIDEBAR INTERCONNECTED UNIVERSAL FILTER MATRIX
    # ----------------------------------------------------
    st.sidebar.header("🎛️ Universal Multi-Filters")
    st.sidebar.markdown("*Filters instantly apply to all charts below.*")
    
    # --- FILTER 1: State Selection ---
    available_states = sorted(list(df['STATE'].dropna().unique()))
    selected_state = st.sidebar.selectbox("1. Filter by State", options=['All States'] + available_states)
    
    # Cascade layer 1
    df_step1 = df if selected_state == 'All States' else df[df['STATE'] == selected_state]
    
    # --- FILTER 2: Provider Number (PROVNUM) Selection ---
    available_provnums = sorted(list(df_step1['PROVNUM'].dropna().unique()))
    selected_provnum = st.sidebar.selectbox("2. Filter by Provider Number (PROVNUM)", options=['All Provider Numbers'] + available_provnums)
    
    # Cascade layer 2
    df_step2 = df_step1 if selected_provnum == 'All Provider Numbers' else df_step1[df_step1['PROVNUM'] == selected_provnum]
    
    # --- FILTER 3: Provider Name (PROVNAME) Selection ---
    available_provnames = sorted(list(df_step2['PROVNAME'].dropna().unique()))
    selected_provname = st.sidebar.selectbox("3. Filter by Provider Name (PROVNAME)", options=['All Provider Names'] + available_provnames)
    
    # Final universally filtered master dataframe
    df_final = df_step2 if selected_provname == 'All Provider Names' else df_step2[df_step2['PROVNAME'] == selected_provname]
    
    if df_final.empty:
        st.warning("⚠️ No facility matches found for this specific combination of criteria. Adjust your sidebar filters.")
    else:
        df_prov = df_final.groupby(['PROVNUM', 'PROVNAME', 'STATE']).agg({
            'Tot_Nurse_hrs': 'mean',
            'MDScensus': 'mean',
            'occup_rate': 'mean',
            'overall_rating': 'max',
            'perm_staff': 'mean',
            'temp_staff': 'mean'
        }).reset_index()

        max_bars = st.sidebar.slider("Max items to view simultaneously", min_value=5, max_value=100, value=20)
        df_prov_sorted = df_prov.sort_values(by='Tot_Nurse_hrs', ascending=False).head(max_bars)

        # ----------------------------------------------------
        # 4. RENDERING VISUAL ANALYTICS GRID
        # ----------------------------------------------------
        
        # --- CHART 1: Tot_Nurse_hrs and MDScensus by PROVNUM ---
        st.subheader("1️⃣ Tot_Nurse_hrs and MDScensus by PROVNUM")
        chart_1_df = df_prov_sorted.set_index('PROVNUM')[['Tot_Nurse_hrs', 'MDScensus']]
        st.line_chart(chart_1_df, use_container_width=True)
        st.markdown("---")

        row2_col1, row2_col2 = st.columns(2)
        
        with row2_col1:
            # --- CHART 2: occup_rate vs PROVNUM ---
            st.subheader("2️⃣ occup_rate vs PROVNUM")
            st.bar_chart(data=df_prov_sorted, x='PROVNUM', y='occup_rate', use_container_width=True)
            
        with row2_col2:
            # --- CHART 3: overall_rating vs PROVNUM ---
            st.subheader("3️⃣ overall_rating vs PROVNUM")
            st.bar_chart(data=df_prov_sorted, x='PROVNUM', y='overall_rating', use_container_width=True)

        st.markdown("---")
        
        # --- CHART 4: perm_staff and temp_staff by PROVNUM ---
        st.subheader("4️⃣ perm_staff and temp_staff by PROVNUM")
        chart_4_df = df_prov_sorted.set_index('PROVNUM')[['perm_staff', 'temp_staff']]
        st.bar_chart(chart_4_df, use_container_width=True)
        st.markdown("---")

        # --- CHART 5: MDScensus per day by PROVNUM and STATE ---
        st.subheader("5️⃣ MDScensus per day by PROVNUM and STATE")
        df_table_view = df_prov_sorted[['STATE', 'PROVNUM', 'PROVNAME', 'MDScensus', 'Tot_Nurse_hrs', 'occup_rate', 'overall_rating']]
        st.dataframe(
            df_table_view.style.background_gradient(cmap="Blues", subset=['MDScensus', 'occup_rate']),
            use_container_width=True
        )

except Exception as e:
    st.error(f"❌ Execution failure within visual layer: {str(e)}")
