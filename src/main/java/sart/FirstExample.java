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
        Dataset<Row> originalTheftsTable = spark.read().option("delimiter", ",").option("header", "true").csv(theftsPath);
        Dataset<Row> originalCars = spark.read().option("delimiter", ",").option("header", "true").csv(carPath);

        // DataSets pre-processing
        Dataset<Row> modelTheftsTable =originalTheftsTable.select("Make/Model" , "Thefts" , "Model Year").withColumnRenamed("Make/Model" , "Car_Model").withColumnRenamed("Model Year" , "Year");
        Dataset<Row> carsTheftsTable =modelTheftsTable.selectExpr("Thefts", "Year" ,"split(Car_Model, ' ')[0] as Car_Model","split(Car_Model, ' ')[1] as Model2");


        carsTheftsTable.show(20);

        // cars table renaming column
        Dataset<Row> carsTable =originalCars.withColumnRenamed("Car Brand" , "Car_Model");
        Dataset<Row> updatedCars = carsTable.join(carsTheftsTable, "Car_Model");

        updatedCars.show(20);


        // DataSets pre-processing dropping nulls values in the table
        Long countF = updatedCars.count();
        System.out.print(countF + " before dropping nulls values ");
        Dataset<Row> carTheftsTable = updatedCars.na().drop();
        long count = carTheftsTable.count();
        System.out.print(count + " after dropping nulls values ");

        // the results show that are no nulls values in the table


        //Partition based in car model column :

        updatedCars.repartition(functions.col("Car_Model")).show(20);

        
        //Thefts Table grouping
     /*   Dataset<Row> theftsTable =carsTheftsTable.groupBy("Car_Model").agg(functions.sum(functions.col("Thefts")));
        theftsTable.show(20);

        // cars table renaming column
        Dataset<Row> carsTable =originalCars.withColumnRenamed("Car Brand" , "Car_Model");
        Dataset<Row> updatedCars = carsTable.join(theftsTable, "Car_Model");

        updatedCars.show(10);
*/

    }
}