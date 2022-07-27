package sart;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class carsThefts{
    public static void main(String[] args) {

        car car = new car();
        final String theftsPath = "src/main/java/sart/data/2015_State_Top10Report_wTotalThefts.csv";
        final String carPath = "src/main/java/sart/data/cars.csv";
        final String updatePath = "src/main/java/sart/data/Updated - Sheet1.csv";


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //create spark session
        SparkSession spark = car.createSparkSession();

    ////////////////////////////////////////////////////////////////////////
        // Read csv files locally

        Dataset<Row> originalTheftsTable = car.readFile(theftsPath , spark);
        Dataset<Row> originalCars = car.readFile(carPath , spark);
        Dataset<Row> updates = car.readFile(updatePath , spark);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // DataSets pre-processing
        Dataset<Row> modelTable2 = car.columnRename(originalTheftsTable , "Make/Model", "Car_Model" );
        modelTable2.show(20);
        Dataset<Row> modelTable = car.columnRename(modelTable2,"Model Year", "Year");
        modelTable.show(10);
        Dataset<Row> modelTheftsTable = modelTable.select("Car_Model", "Year", "Thefts" , "State");

        System.out.print("Thefts Table Data : \n");
        modelTheftsTable.show(5);

        // cars table renaming column
        Dataset<Row> carsTable = car.columnRename(originalCars , "Car Brand", "Car_Brand" );
        System.out.print("cars Table Data : \n");
        carsTable.show(5);

        Dataset<Row> updatedCarTable = car.joinCarTable(carsTable , modelTheftsTable , "Car_Brand", "Car_Model");
        System.out.print("cars and thefts table after join  : \n");
        updatedCarTable.show(50);

        // DataSets checking
        updatedCarTable.count();

        //////////////////////////////////////////////////////////////////////////////////////
        //Partition based in car model column :

        Dataset<Row> partitionDataSet = car.partitioning(updatedCarTable,"Year");
        Dataset<Row> carsTheftsFinal = car.columnRename(partitionDataSet , "Country of Origin", "Origin");
        System.out.print("repartition \n");
        carsTheftsFinal.show(10);

        ////////////////////////////////////////////////////////////////////////////////////

        Dataset<Row> updatesNew = updates.withColumnRenamed("Model Year", "Year2").withColumnRenamed("Thefts", "Thefts2").withColumnRenamed("Make/Model" , "Car_Model2").withColumnRenamed("State" , "State2").cache();

        Dataset<Row> updatesNe2 = carsTheftsFinal.join(updatesNew, carsTheftsFinal.col("State").equalTo(updatesNew.col("State2")).and(carsTheftsFinal.col("Year").equalTo(updatesNew.col("Year2")))
                        .and(carsTheftsFinal.col("Car_Model").equalTo(updatesNew.col("Car_Model2"))),"left")
                .withColumn("Thefts",
                        functions.when(functions.col("Thefts2").isNotNull(), functions.col("Thefts2")).otherwise(functions.col("Thefts"))
                )
                // finally, we drop duplicated
                .drop("Thefts2","State2" , "Car_Model2" , "Year2" , "Rank").cache();

        System.out.print("updated");

    ////////////////////////////////////////////////////////////////////////////////////////////////////

        //most 5 countries from where Americans buy their thefted cars
        Dataset<Row> topThefts = carsTheftsFinal.select("Origin", "Thefts").groupBy("Origin").agg(functions.sum(functions.col("Thefts"))).cache();
        Dataset<Row> csvFile = topThefts.orderBy(functions.col("sum(Thefts)").desc()).cache();

        csvFile.show(5);
    }
}