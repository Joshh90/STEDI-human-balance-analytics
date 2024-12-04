import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1733274960306 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="Step_trainer_landing_node1733274960306")

# Script generated for node Customer_curated
Customer_curated_node1733274963035 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customers_curated", transformation_ctx="Customer_curated_node1733274963035")

# Script generated for node Join
Join_node1733329130320 = Join.apply(frame1=Step_trainer_landing_node1733274960306, frame2=Customer_curated_node1733274963035, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1733329130320")

# Script generated for node Drop Duplicates
DropDuplicates_node1733329223252 =  DynamicFrame.fromDF(Join_node1733329130320.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1733329223252")

# Script generated for node Drop Fields
DropFields_node1733329373491 = DropFields.apply(frame=DropDuplicates_node1733329223252, paths=["`.serialnumber`"], transformation_ctx="DropFields_node1733329373491")

# Script generated for node Target_bucket
EvaluateDataQuality().process_rows(frame=DropFields_node1733329373491, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733274835535", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Target_bucket_node1733275224854 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1733329373491, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lakehouse-and-analytics/step_trainer_trusted/", "partitionKeys": []}, transformation_ctx="Target_bucket_node1733275224854")

job.commit()