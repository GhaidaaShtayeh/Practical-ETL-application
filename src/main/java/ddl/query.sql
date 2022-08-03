
 CREATE EXTERNAL TABLE `document`(
   `id` int,
   `prospect_id` double,
   `brand_id` int,
   `position_id` int,
   `s3_path` string,
   `is_shared_path` boolean,
   `document_lookup_id` int,
  `document_lookup_set_id` int,
   `document_lookup_set_template` string,
   `document_set_id` int,
   `document_set_name` string,
   `state_changed_on` timestamp)
 PARTITIONED BY (
   `nfghfghfhghgfghfghfghfghfghf` int)
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 LOCATION
   's3://taks/yourName/table';