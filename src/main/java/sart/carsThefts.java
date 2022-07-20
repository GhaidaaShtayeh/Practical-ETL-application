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
                .config("spark.sql.crossJoin.enabled", "true")
                .getOrCreate();


        // Read csv files locally
        Dataset<Row> originalTheftsTable = spark.read().option("delimiter", ",").option("header", "true").csv(theftsPath).cache();
        Dataset<Row> originalCars = spark.read().option("delimiter", ",").option("header", "true").csv(carPath).cache();
        Dataset<Row> updates = spark.read().option("delimiter", ",").option("header", "true").csv(updatePath).cache();


        // DataSets pre-processing
        Dataset<Row> modelTable = originalTheftsTable.withColumnRenamed("Make/Model", "Car_Model").withColumnRenamed("Model Year", "Year").cache();
        Dataset<Row> modelTheftsTable = modelTable.select("Car_Model", "Year", "Thefts");

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

        // the results show that are no nulls values in the table

        //Partition based in car model column :
        Dataset<Row> carsTheftsFinal = updatedCarTable.repartition(functions.col("Year")).withColumnRenamed("Country of Origin", "Origin").cache();
        System.out.print("repartition \n");
        carsTheftsFinal.show(500, false);
        /////////////////////////////////////////////////////////////////////////////////////
        /* merge these updates with the original dataset */
        /*   updates.createOrReplaceTempView("updatesTable");
        originalTheftsTable.createOrReplaceTempView("originalTheftsTable");
        Dataset<Row> test = spark.sql( "SELECT * FROM updatesTable JOIN originalTheftsTable ON updatesTable.State = originalTheftsTable.State AND updatesTable.Make/Model = originalTheftsTable.Make/Model AND z1 = z2");
*/
        ////////////////////////////////////////////////////////////////////////////////////
        System.out.print(carsTheftsFinal.count());

        Dataset<Row> updatesNew = updates.withColumnRenamed("Model Year", "Year");
        updatesNew.createOrReplaceTempView("updatesTable");
        carsTheftsFinal.createOrReplaceTempView("carsTheftsFinal2");
        updatesNew.show();
        //  Dataset<Row> test = spark.sql( "ALTER carsTheftsFinal2 set carsTheftsFinal2.Thefts = updatesTable.Thefts from updatesTable where carsTheftsFinal2.Car_Model = updatesTable.Make/Model AND carsTheftsFinal2.State = updatesTable.State AND carsTheftsFinal2.Year = updatesTable.Year ");
        //  test.show(12);

        //most 5 countries from where Americans buy their thefted cars
        Dataset<Row> topThefts = carsTheftsFinal.select("Origin", "Thefts").groupBy("Origin").agg(functions.sum(functions.col("Thefts"))).cache();
        Dataset<Row> csvFile = topThefts.orderBy(functions.col("sum(Thefts)").desc()).cache();
        // csvFile.write().format("com.databricks.spark.csv").option("inferSchema", "false").option("header", true)
        //        .option("charset", "UTF-8").option("delimiter", ",").option("quote", "\'").mode(SaveMode.Overwrite).save("src/main/java/sart/data");
        csvFile.show(5);
    }
}