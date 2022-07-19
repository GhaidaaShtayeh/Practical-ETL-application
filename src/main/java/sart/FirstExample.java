package sart;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.Iterator;

public class FirstExample {
    public static void main(String[] args) {
        final  String theftsPath = "src/main/java/sart/2015_State_Top10Report_wTotalThefts.csv";
        final  String carPath = "src/main/java/sart/cars.csv";

        //create spark session
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("cars")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // Read csv files locally
        Dataset<Row> originalTheftsTable = spark.read().option("delimiter", ",").option("header", "true").csv(theftsPath).cache();
        Dataset<Row> originalCars = spark.read().option("delimiter", ",").option("header", "true").csv(carPath).cache();

        // DataSets pre-processing
        Dataset<Row> modelTheftsTable =originalTheftsTable.withColumnRenamed("Make/Model" , "Car_Model").withColumnRenamed("Model Year" , "Year").cache();

        System.out.print("Thefts Table Data : ");
        modelTheftsTable.show(5);

        // cars table renaming column
        Dataset<Row> carsTable =originalCars.withColumnRenamed("Car Brand" , "Car_Brand").cache();

        System.out.print("cars Table Data : ");
        carsTable.show(5);

        Dataset<Row> updatedCarTable =  carsTable.join(modelTheftsTable)
                .filter(modelTheftsTable.col("Car_Model").contains(carsTable.col("Car_Brand"))).cache();

        System.out.print("cars and thefts table after join  : ");
        updatedCarTable.show(50);

        // DataSets pre-processing dropping nulls values in the table
        Long countF = updatedCarTable.count();
        System.out.print(countF + " before dropping nulls values ");
        Dataset<Row> carTheftsTable = updatedCarTable.na().drop();
        long count = carTheftsTable.count();
        System.out.print(count + " after dropping nulls values ");

        // the results show that are no nulls values in the table


        //Partition based in car model column :

        Dataset<Row> carsTheftsFinal = updatedCarTable.repartition(functions.col("Car_Brand")).cache();
        System.out.print("cars and thefts table after repartitioning according to Car_Brand column  : ");
        carsTheftsFinal.show(20 , false);

        carsTheftsFinal.join(carsTable,"Car_Brand").show(50,false);



    }
}