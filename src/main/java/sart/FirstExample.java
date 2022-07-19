package sart;
import org.apache.spark.sql.*;
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
        Dataset<Row> modelTable =originalTheftsTable.withColumnRenamed("Make/Model" , "Car_Model").withColumnRenamed("Model Year" , "Year").cache();
        Dataset<Row> modelTheftsTable =modelTable.select("Car_Model");

        System.out.print("Thefts Table Data : ");
        modelTheftsTable.show(5);

        // cars table renaming column
        Dataset<Row> carsTable = originalCars.withColumnRenamed("Car Brand" , "Car_Brand").cache();

        System.out.print("cars Table Data : ");
        carsTable.show(5);

        Dataset<Row> updatedCarTable =  carsTable.join(modelTheftsTable,(modelTheftsTable.col("Car_Model").contains(carsTable.col("Car_Brand")))).cache();

        System.out.print("cars and thefts table after join  : ");
        updatedCarTable.show(50);

        // DataSets checking
        long count = updatedCarTable.count();
        System.out.print(count + " :  after || before : " + originalTheftsTable.count());

        // the results show that are no nulls values in the table

        //Partition based in car model column :
        Dataset<Row> carsTheftsFinal = updatedCarTable.repartition(functions.col("Car_Brand")).withColumnRenamed("Country of Origin" , "Origin").cache();
        System.out.print("repartition");
        carsTheftsFinal.show(20 , false);

        Dataset<Row> carsTheftsFinal2 =carsTheftsFinal.join(modelTable,"Car_Model").cache();
         carsTheftsFinal2.show(35,false);

        //most 5 countries from where Americans buy their thefted cars
            Dataset<Row> topThefts = carsTheftsFinal2.select("Origin","Thefts").groupBy("Origin").agg(functions.sum(functions.col("Thefts"))).cache();
        //    topThefts.orderBy(functions.col("sum(Thefts)").desc()).write().save("data.csv");
             topThefts.show(5);
    }
}