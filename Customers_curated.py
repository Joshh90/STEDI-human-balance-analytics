import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer_trusted
Customer_trusted_node1732955688216 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-and-analytics/Customer_trusted/"], "recurse": True}, transformation_ctx="Customer_trusted_node1732955688216")

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1732955686485 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-and-analytics/accelerometer_landing/"], "recurse": True}, transformation_ctx="Accelerometer_landing_node1732955686485")

# Script generated for node Join
Join_node1733033934776 = Join.apply(frame1=Customer_trusted_node1732955688216, frame2=Accelerometer_landing_node1732955686485, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1733033934776")

# Script generated for node Drop Duplicates
DropDuplicates_node1732956414095 =  DynamicFrame.fromDF(Join_node1733033934776.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1732956414095")

# Script generated for node Drop Fields
DropFields_node1732956418808 = DropFields.apply(frame=DropDuplicates_node1732956414095, paths=["x", "y", "z", "user", "timestamp"], transformation_ctx="DropFields_node1732956418808")

# Script generated for node Customers_curated
Customers_curated_node1732956621880 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1732956418808, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lakehouse-and-analytics/Customers_curated/", "partitionKeys": []}, transformation_ctx="Customers_curated_node1732956621880")

job.commit()