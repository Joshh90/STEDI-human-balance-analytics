
CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorreadingtime` timestamp COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `serialnumber` varchar(65535) COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lakehouse-and-analytics/step_trainer_landing'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1732738573')
