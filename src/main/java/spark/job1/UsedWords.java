package spark.job1;

import groovy.lang.Tuple;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

import scala.Tuple2;
import spark.ConstantFields;


public class UsedWords {

    private String regex = "[^A-Za-z0-9]";
    private static String out = "/home/fabrizio/Scaricati/result";
    private String path;

    public UsedWords (String path){
        this.path=path;
    }

    public JavaPairRDD<Long, String> mostUsedWords() throws IOException {
        SparkConf conf = new SparkConf().setAppName("Most Used Words");
        JavaSparkContext context = new JavaSparkContext(conf);

        CSVParser parser = CSVParser.parse(new File(this.path), Charset.defaultCharset(), CSVFormat.DEFAULT);

        JavaRDD<CSVRecord> reviews = context.textFile(path).
                map(line -> CSVParser.parse(line, CSVFormat.DEFAULT).getRecords().get(0));

        JavaPairRDD<Long, String> year2word =
                reviews.flatMapToPair(rev -> (getWordFromText(rev.get(ConstantFields.Summary)))
                                .stream().map(w -> (new Tuple2<>(getYear(rev.get(ConstantFields.Time)), w)))
                                .collect(Collectors.toList()).iterator());

        return year2word;

    }



    public List<String> getWordFromText (String text){
        String s = text.toLowerCase().replaceAll(regex, " ");
        List<String> words = Arrays.asList(s.split(" "));
        return words;
    }

    public long getYear (String timeStamp) {
        long time = Long.parseLong(timeStamp)*1000L;
        Date date = new Date(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR);
    }

}
