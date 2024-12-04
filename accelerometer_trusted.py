import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer_trusted
Customer_trusted_node1732950422450 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-and-analytics/Customer_trusted/"], "recurse": True}, transformation_ctx="Customer_trusted_node1732950422450")

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1732950425549 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-and-analytics/accelerometer_landing/"], "recurse": True}, transformation_ctx="Accelerometer_landing_node1732950425549")

# Script generated for node Join
Join_node1732950436416 = Join.apply(frame1=Accelerometer_landing_node1732950425549, frame2=Customer_trusted_node1732950422450, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1732950436416")

# Script generated for node Drop Fields
DropFields_node1732953159308 = DropFields.apply(frame=Join_node1732950436416, paths=["phone", "email"], transformation_ctx="DropFields_node1732953159308")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1732950692683 = glueContext.getSink(path="s3://stedi-lakehouse-and-analytics/Accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1732950692683")
accelerometer_trusted_node1732950692683.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1732950692683.setFormat("json")
accelerometer_trusted_node1732950692683.writeFrame(DropFields_node1732953159308)
job.commit()