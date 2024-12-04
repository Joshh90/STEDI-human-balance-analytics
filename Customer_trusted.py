{
	"jobConfig": {
		"name": "Customer_landing_to_trusted",
		"description": "",
		"role": "arn:aws:iam::116981777358:role/my-glue-service-role",
		"command": "glueetl",
		"version": "4.0",
		"runtime": null,
		"workerType": "G.1X",
		"numberOfWorkers": 10,
		"maxCapacity": 10,
		"jobRunQueuingEnabled": false,
		"maxRetries": 0,
		"timeout": 2880,
		"maxConcurrentRuns": 1,
		"security": "none",
		"scriptName": "Customer_landing_to_trusted.py",
		"scriptLocation": "s3://aws-glue-assets-116981777358-us-east-1/scripts/",
		"language": "python-3",
		"spark": true,
		"sparkConfiguration": "standard",
		"jobParameters": [],
		"tags": [],
		"jobMode": "DEVELOPER_MODE",
		"createdOn": "2024-11-30T06:18:52.186Z",
		"developerMode": true,
		"connectionsList": [],
		"temporaryDirectory": "s3://aws-glue-assets-116981777358-us-east-1/temporary/",
		"logging": true,
		"glueHiveMetastore": true,
		"etlAutoTuning": true,
		"metrics": true,
		"observabilityMetrics": true,
		"bookmark": "job-bookmark-disable",
		"sparkPath": "s3://aws-glue-assets-116981777358-us-east-1/sparkHistoryLogs/",
		"flexExecution": false,
		"minFlexWorkers": null,
		"maintenanceWindow": null,
		"dataLineage": false
	},
	"hasBeenSaved": false,
	"usageProfileName": null,
	"script": "import sys\nfrom awsglue.transforms import *\nfrom awsglue.utils import getResolvedOptions\nfrom pyspark.context import SparkContext\nfrom awsglue.context import GlueContext\nfrom awsglue.job import Job\n\n# Get job arguments\nargs = getResolvedOptions(sys.argv, ['JOB_NAME'])\nsc = SparkContext()\nglueContext = GlueContext(sc)\nspark = glueContext.spark_session\njob = Job(glueContext)\njob.init(args['JOB_NAME'], args)\n\n# Script generated for node Amazon S3\nAmazonS3_node1732945213259 = glueContext.create_dynamic_frame.from_options(\n    format_options={\"multiLine\": \"false\"},\n    connection_type=\"s3\",\n    format=\"json\",\n    connection_options={\"paths\": [\"s3://stedi-lakehouse-and-analytics/customer_landing/\"], \"recurse\": True},\n    transformation_ctx=\"AmazonS3_node1732945213259\"\n)\n\n# Script generated for node ApplyMapping (replacing Privacy Filter)\nApplyMapping_node2 = Filter.apply(\n    frame=AmazonS3_node1732945213259,\n    f=lambda row: (row.get(\"shareWithResearchAsOfDate\") and row[\"shareWithResearchAsOfDate\"] != 0),\n    transformation_ctx=\"ApplyMapping_node2\"\n)\n\n# Script generated for node Trusted Customer Zone\nTrusted_Customer_Zone_node1732946064159 = glueContext.write_dynamic_frame.from_options(\n    frame=ApplyMapping_node2,\n    connection_type=\"s3\",\n    format=\"json\",\n    connection_options={\n        \"path\": \"s3://stedi-lakehouse-and-analytics/Customer_trusted/\",\n        \"partitionKeys\": []\n    },\n    transformation_ctx=\"Trusted_Customer_Zone_node1732946064159\"\n)\n\n# Commit the Glue job\njob.commit()"
}