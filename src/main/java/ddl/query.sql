
create database cars_thefts;

use cars_thefts ;


 CREATE EXTERNAL TABLE 'cars'(
   'Car_Brand' string,
   'Country' string,
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  TBLPROPERTIES ("skip.header.line.count"="1");

  LOAD DATA LOCAL INPATH "C:/Users/DELL/Documents/Practical-ETL-application55/src/main/java/sart/data" into table cars;


 Select Car_Model  sum(thefts) as sum_thefts, Car_Brand from cars_thefts.carsTheftsWithUpdate Group By Car_Brand, Car_Model SORT BY sum_thefts DESC LIMIT 5

 Select sum(thefts) as sum_thefts , state  from car_theft.carsTheftsWithUpdate  Group By State SORT BY sum_thefts DESC LIMIT 5

