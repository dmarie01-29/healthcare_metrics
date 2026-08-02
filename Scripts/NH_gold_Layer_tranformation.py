import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import traceback

# Explicit imports for the Data Quality Framework
from awsgluedq.transforms.evaluate_data_quality import EvaluateDataQuality

def sparkSqlQuery(glueContext, spark_session, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark_session.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DEFAULT_DATA_QUALITY_RULESET = """
Rules = [ ColumnCount > 0 ]
"""

try:
    print("⚡ STEP 1: Ingesting data sources from S3...")
    Source_US_Averages_node1784662025862 = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "encoding": "ISO-8859-1"}, 
        connection_type="s3", 
        format="csv", 
        connection_options={"paths": ["s3://glueproject-bucket-871049984307-us-east-1-an/bronze/NH_StateUSAverages_Oct2024_Cleaned.csv"]}, 
        transformation_ctx="Source_US_Averages_node1784662025862"
    )

    Source_Provider_Info_node1784658996308 = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "encoding": "ISO-8859-1"}, 
        connection_type="s3", 
        format="csv", 
        connection_options={"paths": ["s3://glueproject-bucket-871049984307-us-east-1-an/bronze/NH_ProviderInfo_Oct2024_Cleaned.csv"]}, 
        transformation_ctx="Source_Provider_Info_node1784658996308"
    )

    Source_staffing_node1784658890684 = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "encoding": "ISO-8859-1"}, 
        connection_type="s3", 
        format="csv", 
        connection_options={"paths": ["s3://glueproject-bucket-871049984307-us-east-1-an/bronze/PBJ_Daily_Nurse_Staffing_Q2_2024_Cleaned.csv"]}, 
        transformation_ctx="Source_staffing_node1784658890684"
    )

    print("⚡ STEP 2: Building robust SQL mapping query with occupancy metrics...")
    SqlQuery0 = '''
    SELECT 
        s.*,
        
        (CAST(COALESCE(s.Hrs_RN, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_LPN, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_CNA, 0) AS DOUBLE)) AS Tot_Nurse_hrs,
        
        (CAST(COALESCE(s.Hrs_RNadmin_emp, 0) AS DOUBLE) + CAST(COALESCE(s.Hrs_RN_emp, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_LPNadmin_emp, 0) AS DOUBLE) + CAST(COALESCE(s.Hrs_LPN_emp, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_CNA_emp, 0) AS DOUBLE) + CAST(COALESCE(s.Hrs_NAtrn_emp, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_MedAide_emp, 0) AS DOUBLE)) AS perm_staff,
         
        (CAST(COALESCE(s.Hrs_RNadmin_ctr, 0) AS DOUBLE) + CAST(COALESCE(s.Hrs_RN_ctr, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_LPNadmin_ctr, 0) AS DOUBLE) + CAST(COALESCE(s.Hrs_LPN_ctr, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_CNA_ctr, 0) AS DOUBLE) + CAST(COALESCE(s.Hrs_NAtrn_ctr, 0) AS DOUBLE) + 
         CAST(COALESCE(s.Hrs_MedAide_ctr, 0) AS DOUBLE)) AS temp_staff,
         
        p.`number of certified beds` AS occup_limit,
        st.`Average Number of Residents per Day` AS Avg_residents_per_day,
        st.`Reported Total Nurse Staffing Hours per Resident per Day` AS Avg_tot_nurse_hrs_per_resident_per_day,
        
        100 * (CAST(COALESCE(s.MDScensus, 0) AS DOUBLE) / CAST(p.`number of certified beds` AS DOUBLE)) AS occup_rate,
        p.`Overall Rating` AS overall_rating

    FROM Staffing s
    LEFT JOIN Provider_Info p ON s.PROVNUM = p.`CMS Certification Number (CCN)`
    LEFT JOIN State_Averages st ON p.State = st.`State or Nation`
    '''

    print("⚡ STEP 3: Executing SQL in-memory transformations...")
    SQLQuery_node1784737981230 = sparkSqlQuery(
        glueContext, 
        spark_session = spark,
        query = SqlQuery0, 
        mapping = {
            "Staffing": Source_staffing_node1784658890684, 
            "Provider_Info": Source_Provider_Info_node1784658996308, 
            "State_Averages": Source_US_Averages_node1784662025862
        }, 
        transformation_ctx = "SQLQuery_node1784737981230"
    )

    print("⚡ STEP 4: Evaluating Data Quality rules...")
    EvaluateDataQuality().process_rows(
        frame=SQLQuery_node1784737981230, 
        ruleset=DEFAULT_DATA_QUALITY_RULESET, 
        publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1785156380869", "enableDataQualityResultsPublishing": True}, 
        additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
    )

    print("⚡ STEP 5: Consolidating partitions and streaming clean data straight to S3 Gold...")
    single_output_df = SQLQuery_node1784737981230.toDF().coalesce(1)
    final_gold_frame = DynamicFrame.fromDF(single_output_df, glueContext, "final_gold_frame")

    AmazonS3_node1785157691950 = glueContext.write_dynamic_frame.from_options(
        frame=final_gold_frame, 
        connection_type="s3", 
        format="csv", 
        connection_options={
            "path": "s3://glueproject-bucket-871049984307-us-east-1-an/gold/", 
            "partitionKeys": []
        }, 
        transformation_ctx="AmazonS3_node1785157691950"
    )

    print("🚀 SUCCESS: Gold Layer file updated with occupancy calculations successfully!")
    job.commit()

except Exception as e:
    print("❌ CRITICAL SCRIPT BREAKDOWN DETECTED:")
    print(traceback.format_exc())
    raise e
