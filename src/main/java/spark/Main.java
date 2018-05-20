package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import utilities.LoadData;

public class Main {
    public static void main(String[] args){
        // load data from csv to RDD
        JavaRDD<Row> rdd = LoadData.readCsvToRDD();

        // execute jobs
        Job1_MostUsedWords.mostUsedWords(rdd);
        Job2_ScorePerYear.scorePerYear(rdd);
        Job3_CommonUsers.commonUsers(rdd);
    }

}
