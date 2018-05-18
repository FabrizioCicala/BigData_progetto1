package utilities;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadData {

    private static final String path = "/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/Reviews.csv";

    public static JavaRDD<Row> readCsvToRDD (){
        SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[4]");
        SparkSession spark = SparkSession
                .builder()
                .appName("Most Used Words")
                .config(conf)
                .getOrCreate();

        Dataset<Row> df = spark.read().option("header", "true").csv(path);
        return df.javaRDD();
    }
}
