package spark;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import utilities.*;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * 1. Un job che sia in grado di generare, per ciascun anno, le dieci parole che sono state più usate
 * nelle recensioni (campo summary) in ordine di frequenza, indicando, per ogni parola, la sua
 * frequenza, ovvero il numero di occorrenze della parola nelle recensioni di quell’anno.
 **/
public class Job1_MostUsedWords {

    public static String mostUsedWords(JavaRDD<Row> rdd) {

        long startTime = System.currentTimeMillis();

        // PairRDD: anno, lista (word) - dati relativi alle singole recensioni
        JavaPairRDD<Integer, List<String>> year2reviewSummary =
                rdd.mapToPair(row -> new Tuple2<>(ParseTime.getYear((String)row.get(ConstantFields.Time)),
                        ParseText.getWordFromText((String)row.get(ConstantFields.Summary))));

        // PairRDD: (anno, word), count
        JavaPairRDD<Tuple2<Integer, String>, Long> year2word =
                year2reviewSummary.flatMapToPair(tuple -> tuple._2.stream().map(word -> new Tuple2<>(new Tuple2<>(tuple._1, word), 1L))
                .iterator()).reduceByKey((a, b) -> a +b);

        // PairRDD: anno, (word, count)
        JavaPairRDD<Integer, Tuple2<String, Long>>  year2wordCount =
                year2word.mapToPair(tuple -> new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._1._2, tuple._2)));

        // PairRDD: anno, lista (word, count)
        JavaPairRDD<Integer, Iterable<Tuple2<String, Long>>> allWords = year2wordCount.groupByKey().sortByKey();

        // PairRDD: anno, lista (word, count) ordinato e limitati a 10 elementi
        JavaPairRDD<Integer, List<Tuple2<String, Long>>> mostUsedWords =
                allWords.mapToPair(tuple -> new Tuple2<>(
                        tuple._1,
                        Lists.newArrayList(tuple._2).stream().
                                sorted(TupleComparator::compareByLongValues)
                                .limit(10)
                                .collect(toList()))).filter(tuple -> tuple._1!=0);

//        mostUsedWords.coalesce(1, true).saveAsTextFile("/home/fabrizio/Scaricati/spark_job1_result");

        List<Tuple2<Integer, List<Tuple2<String, Long>>>> tuples = mostUsedWords.collect();

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        return ("\nJob 1: " + totalTime + " sec");
    }



}
