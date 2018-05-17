package spark.job1;

import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;

public class Main {
    static final String path = "/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/provaReviews.csv";

    public static void main (String[] args) throws IOException {
        UsedWords uw = new UsedWords(path);
        System.out.println(uw.mostUsedWords().toString());
    }
}
