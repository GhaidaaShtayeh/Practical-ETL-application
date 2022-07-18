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

        String theftsPath = "src/main/java/sart/2015_State_Top10Report_wTotalThefts.csv";
        String carPath = "src/main/java/sart/cars.csv";

        //create spark session
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("cars")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // Read csv files locally
        Dataset<Row> originalTheftsTable = spark.read().option("delimiter", ",").option("header", "true").csv(theftsPath);
        Dataset<Row> originalCars = spark.read().option("delimiter", ",").option("header", "true").csv(carPath);

        // DataSets pre-processing
        Dataset<Row> modelTheftsTable =originalTheftsTable.select("Make/Model" , "Thefts").withColumnRenamed("Make/Model" , "Car_Model");
        Dataset<Row> carsTheftsTable =modelTheftsTable.selectExpr("Thefts","split(Car_Model, ' ')[0] as Car_Model");


        //Thefts Table grouping
        Dataset<Row> theftsTable =carsTheftsTable.groupBy("Car_Model").agg(functions.sum(functions.col("Thefts"))).as("Thefts");
        theftsTable.show(20);

        // cars table renaming column
        Dataset<Row> carsTable =originalCars.withColumnRenamed("Car Brand" , "Car_Model");
        Dataset<Row> updatedCars = carsTable.join(theftsTable, "Car_Model");

        updatedCars.show(10);

    }
}