package sart;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class carsThefts {
    public static void main(String[] args) {
        final String theftsPath = "src/main/java/sart/data/2015_State_Top10Report_wTotalThefts.csv";
        final String carPath = "src/main/java/sart/data/cars.csv";
        final String updatePath = "src/main/java/sart/data/Updated - Sheet1.csv";


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //create spark session
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("cars")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

    ////////////////////////////////////////////////////////////////////////
        // Read csv files locally
        Dataset<Row> originalTheftsTable = spark.read().option("delimiter", ",").option("header", "true").csv(theftsPath).cache();
        Dataset<Row> originalCars = spark.read().option("delimiter", ",").option("header", "true").csv(carPath).cache();
        Dataset<Row> updates = spark.read().option("delimiter", ",").option("header", "true").csv(updatePath).cache();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // DataSets pre-processing
        Dataset<Row> modelTable = originalTheftsTable.withColumnRenamed("Make/Model", "Car_Model").withColumnRenamed("Model Year", "Year").cache();
        Dataset<Row> modelTheftsTable = modelTable.select("Car_Model", "Year", "Thefts" , "State");

        System.out.print("Thefts Table Data : \n");
        modelTheftsTable.show(5);

        // cars table renaming column
        Dataset<Row> carsTable = originalCars.withColumnRenamed("Car Brand", "Car_Brand").cache();

        System.out.print("cars Table Data : \n");
        carsTable.show(5);

        Dataset<Row> updatedCarTable = carsTable.join(modelTheftsTable, (modelTheftsTable.col("Car_Model").contains(carsTable.col("Car_Brand")))).cache();

        System.out.print("cars and thefts table after join  : \n");
        updatedCarTable.show(50);

        // DataSets checking
        long count = updatedCarTable.count();
        System.out.print(count + " :  after || before : " + originalTheftsTable.count() + "\n");

        //////////////////////////////////////////////////////////////////////////////////////
        //Partition based in car model column :
        Dataset<Row> carsTheftsFinal = updatedCarTable.repartition(functions.col("Year")).withColumnRenamed("Country of Origin", "Origin").cache();
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