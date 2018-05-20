package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import utilities.LoadData;

public class SparkMain {
    public static void main(String[] args){
        // load data from csv to RDD
        JavaRDD<Row> rdd = LoadData.readCsvToRDD();

        // execute jobs
        String job1 = Job1_MostUsedWords.mostUsedWords(rdd);
        String job2 = Job2_ScorePerYear.scorePerYear(rdd);
        String job3= Job3_CommonUsers.commonUsers(rdd);

        System.out.println("Spark - Tempi di esecuzione");
        System.out.println(job1);
        System.out.println(job2);
        System.out.println(job3);
    }

}
