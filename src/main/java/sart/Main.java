package sart;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import sart.Service.Car;

public class Main{
    public static void main(String[] args) {

        Car temp = new Car();

        final String theftsPath = "src/main/java/sart/data/2015_State_Top10Report_wTotalThefts.csv";
        final String carPath = "src/main/java/sart/data/cars.csv";
        final String updatePath = "src/main/java/sart/data/Updated - Sheet1.csv";


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //create spark session
        SparkSession spark = temp.createSparkSession();

    ////////////////////////////////////////////////////////////////////////
        // Read csv files locally

        Dataset<Row> originalTheftsTable = temp.readFile(theftsPath , spark);
        Dataset<Row> originalCars = temp.readFile(carPath , spark);
        Dataset<Row> updates = temp.readFile(updatePath , spark);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // DataSets pre-processing
        Dataset<Row> modelTable2 = temp.columnRename(originalTheftsTable , "Make/Model", "Car_Model" );
        Dataset<Row> modelTable = temp.columnRename(modelTable2,"Model Year", "Year");

        //column selection from model car Table extract a dataset which contains the car model, year, Thefts and state (from the
        //dataset)
        Dataset<Row> modelTheftsTable = modelTable.select("Car_Model", "Year", "Thefts" , "State");


        // cars table renaming column
        Dataset<Row> carsTable = temp.columnRename(originalCars , "Car Brand", "Car_Brand" );

        // extract a dataset which contains the car model (from the
        //dataset) and the country of origin of this car from another data set
        // joined 2 tables cars with thefts and model and country
        Dataset<Row> updatedCarTable = temp.joinCarTable(carsTable , modelTheftsTable , "Car_Brand", "Car_Model");
        System.out.print("cars and thefts table after join  : \n");
        updatedCarTable.show(50);

        // DataSets checking
        temp.dataSetCount(updatedCarTable);

        //////////////////////////////////////////////////////////////////////////////////////
        //Partition based in temp model column :
        Dataset<Row> partitionDataSet = temp.partitioning(updatedCarTable,"Year");
        Dataset<Row> carsTheftsFinal = temp.columnRename(partitionDataSet , "Country of Origin", "Origin");
        System.out.print("repartition \n");
        carsTheftsFinal.show(10);

        ////////////////////////////////////////////////////////////////////////////////////

        Dataset<Row> updatesNew = updates.withColumnRenamed("Model Year", "Year2").withColumnRenamed("Thefts", "Thefts2").withColumnRenamed("Make/Model" , "Car_Model2").withColumnRenamed("State" , "State2").cache();

        Dataset<Row> updatesNe2 = temp.updateCarTable(updatesNew,carsTheftsFinal);
        updatesNe2.show(15);
        System.out.print("updated");

    ////////////////////////////////////////////////////////////////////////////////////////////////////

        //most 5 countries from where Americans buy their thefted cars
        Dataset<Row> csvFile = temp.topCountries(carsTheftsFinal);

        temp.saveAsCSV(csvFile);
        csvFile.show(5);
    }
}