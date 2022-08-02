package sart.Service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import sart.Interface.IFile;

public class Car implements IFile {

    public SparkSession createSparkSession(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("cars")
                .config("spark.some.config.option", "some-value")
                .config("spark.sql.crossJoin.enabled", true)
                .getOrCreate();
        return spark;
    }
    @Override
    public Dataset<Row> readFile(String path , SparkSession spark) {
        Dataset<Row> dataset = spark.read().option("delimiter", ",").option("header", "true").csv(path).cache();
        return dataset;
    }
    @Override
    public Dataset<Row> columnRename (Dataset<Row> dataset , String oldName , String newName){
        Dataset<Row> newDataSet = dataset.withColumnRenamed(oldName, newName).cache();
        return newDataSet;
    }
    @Override
    public void dataSetCount(Dataset<Row> dataset) {
       long count = dataset.count();
        System.out.print(count + " :  count of dataset "+"\n");

    }
    @Override
    public Dataset<Row> partitioning ( Dataset<Row> datasetOriginal , String partitionColumn){
        Car car = new Car();
        Dataset<Row> partioinDataSet = datasetOriginal.repartition(functions.col(partitionColumn));
        return partioinDataSet;
    }

    @Override
    public void saveAsCSV(Dataset<Row> dataset) {
        dataset.coalesce(1).write().option("header","true").format("csv").save("src/main/java/sart/topFiveCountries");
    }

    public Dataset<Row> joinCarTable(Dataset<Row> originalData , Dataset<Row> joinedTable , String carColumnName , String joinedTableColumnName ){
        Dataset<Row> updatedCarTable = originalData.join(joinedTable, (joinedTable.col(joinedTableColumnName).contains(originalData.col(carColumnName)))).cache();
        return updatedCarTable;
    }
    public Dataset<Row> updateCarTable(Dataset<Row> updatedDataset , Dataset<Row> originalDataset){

        Dataset<Row> finalDataset = originalDataset.join(updatedDataset, originalDataset.col("State").equalTo(updatedDataset.col("State2")).and(originalDataset.col("Year").equalTo(updatedDataset.col("Year2")))
                        .and(originalDataset.col("Car_Model").equalTo(updatedDataset.col("Car_Model2"))),"left")
                .withColumn("Thefts",
                        functions.when(functions.col("Thefts2").isNotNull(), functions.col("Thefts2")).otherwise(functions.col("Thefts"))
                )
                // finally, we drop duplicated
                .drop("Thefts2","State2" , "Car_Model2" , "Year2" , "Rank").cache();
        return finalDataset;
    }
    public Dataset<Row> topCountries(Dataset<Row> dataset){
        Dataset<Row> selectedCountingDataset = dataset.select("Origin", "Thefts").groupBy("Origin").agg(functions.sum(functions.col("Thefts"))).cache();
        Dataset<Row> finalDataset = selectedCountingDataset.orderBy(functions.col("sum(Thefts)").desc()).cache();
        return finalDataset;
    }
}
