import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

# --- FIXED FUNCTION: Added global spark session access inside scope ---
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    # Explicitly reference the active spark instance initialized below
    global spark 
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [ ColumnCount > 0 ]
"""

# Script generated for node Source_US_Averages
Source_US_Averages_node1784662025862 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": ["s3://glueproject-bucket-871049984307-us-east-1-an/bronze/NH_StateUSAverages_Oct2024_Cleaned.csv"]}, 
    transformation_ctx="Source_US_Averages_node1784662025862"
)

# Script generated for node Source_Provider_Info
Source_Provider_Info_node1784658996308 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": ["s3://glueproject-bucket-871049984307-us-east-1-an/bronze/NH_ProviderInfo_Oct2024_Cleaned.csv"]}, 
    transformation_ctx="Source_Provider_Info_node1784658996308"
)

# Script generated for node Source_staffing
Source_staffing_node1784658890684 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": ["s3://glueproject-bucket-871049984307-us-east-1-an/bronze/PBJ_Daily_Nurse_Staffing_Q2_2024_Cleaned.csv"]}, 
    transformation_ctx="Source_staffing_node1784658890684"
)

# --- FIXED SQL QUERY: Standardized casing to match mapping exactly ---
SqlQuery0 = '''
SELECT 
    s.*,
    (s.Hrs_RN::DOUBLE + s.Hrs_LPN::DOUBLE + s.Hrs_CNA::DOUBLE) AS Tot_Nurse_hrs,
    
    (s.Hrs_RNadmin_emp::DOUBLE + s.Hrs_RN_emp::DOUBLE + s.Hrs_LPNadmin_emp::DOUBLE + 
     s.Hrs_LPN_emp::DOUBLE + s.Hrs_CNA_emp::DOUBLE + s.Hrs_NAtrn_emp::DOUBLE + 
     s.Hrs_MedAide_emp::DOUBLE) AS perm_staff,
     
    (s.Hrs_RNadmin_ctr::DOUBLE + s.Hrs_RN_ctr::DOUBLE + s.Hrs_LPNadmin_ctr::DOUBLE + 
     s.Hrs_LPN_ctr::DOUBLE + s.Hrs_CNA_ctr::DOUBLE + s.Hrs_NAtrn_ctr::DOUBLE + 
     s.Hrs_MedAide_ctr::DOUBLE) AS temp_staff,
     
    p.`number of certified beds` AS occup_limit,
    st.`Average Number of Residents per Day` AS Avg_residents_per_day,
    st.`Reported Total Nurse Staffing Hours per Resident per Day` AS Avg_tot_nurse_hrs_per_resident_per_day

FROM Staffing s

LEFT JOIN Provider_Info p 
    ON s.Provnum = p.`provider number`

LEFT JOIN State_Averages st 
    ON p.State = st.State
'''

# Mapping keys now match the SQL tables exactly
SQLQuery_node1784737981230 = sparkSqlQuery(
    glueContext, 
    query = SqlQuery0, 
    mapping = {
        "Staffing": Source_staffing_node1784658890684, 
        "Provider_Info": Source_Provider_Info_node1784658996308, 
        "State_Averages": Source_US_Averages_node1784662025862
    }, 
    transformation_ctx = "SQLQuery_node1784737981230"
)

# Script generated for node Amazon S3 Data Quality Check
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node1784737981230, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1785156380869", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Write output safely into S3 gold folder
AmazonS3_node1785157691950 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1784737981230, 
    connection_type="s3", 
    format="csv", 
    connection_options={"path": "s3://glueproject-bucket-871049984307-us-east-1-an/gold/", "partitionKeys": []}, 
    transformation_ctx="AmazonS3_node1785157691950"
)

job.commit()
