package sart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class car implements IFile{
    Dataset<Row> carData;

    public SparkSession createSparkSession(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("cars")
                .config("spark.some.config.option", "some-value")
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
        car car = new car();
        Dataset<Row> partioinDataSet = datasetOriginal.repartition(functions.col(partitionColumn));
        return partioinDataSet;
    }
    public Dataset<Row> joinCarTable(Dataset<Row> originalData , Dataset<Row> joinedTable , String carColumnName , String joinedTableColumnName ){
        Dataset<Row> updatedCarTable = originalData.join(joinedTable, (joinedTable.col(joinedTableColumnName).contains(originalData.col(carColumnName)))).cache();
        return updatedCarTable;
    }
}
