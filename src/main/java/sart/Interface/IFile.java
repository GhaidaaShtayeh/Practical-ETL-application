package sart.Interface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface IFile {
    public Dataset<Row> readFile(String path , SparkSession spark);
    public Dataset<Row> columnRename (Dataset<Row> dataset , String oldColumnName , String newColumnName);
    public void dataSetCount(Dataset<Row> dataset);
    public Dataset<Row> partitioning ( Dataset<Row> originalDataset , String partitionColumn);
    public void saveAsCSV (Dataset<Row> dataset);
}
