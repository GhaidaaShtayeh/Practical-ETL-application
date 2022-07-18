Create table car
 (id int, car_brand VARCHAR , country_of_origin VARCHAR)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (   'separatorChar'=','  ,'quoteChar'    ='\"' )
 STORED AS TEXTFILE
 location 'hdfs:///user/harssing/bds/' TBLPROPERTIES('skip.header.line.count'='1');