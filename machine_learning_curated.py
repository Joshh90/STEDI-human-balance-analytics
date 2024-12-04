import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
Step_trainer_landing_node1733327256555 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-and-analytics/step_trainer_landing/"], "recurse": True}, transformation_ctx="Step_trainer_landing_node1733327256555")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1733326989032 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-and-analytics/Accelerometer_trusted/"], "recurse": True}, transformation_ctx="Accelerometer_trusted_node1733326989032")

# Script generated for node Join
Join_node1733327321095 = Join.apply(frame1=Accelerometer_trusted_node1733326989032, frame2=Step_trainer_landing_node1733327256555, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1733327321095")

# Script generated for node Machine_learning_curated
EvaluateDataQuality().process_rows(frame=Join_node1733327321095, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733326896053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Machine_learning_curated_node1733327369829 = glueContext.write_dynamic_frame.from_options(frame=Join_node1733327321095, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lakehouse-and-analytics/machine_learning_curated/", "partitionKeys": []}, transformation_ctx="Machine_learning_curated_node1733327369829")

job.commit()