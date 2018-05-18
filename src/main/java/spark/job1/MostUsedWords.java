package spark.job1;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import utilities.ConstantFields;
import utilities.LoadData;
import utilities.ParseText;
import utilities.ParseTime;

import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class MostUsedWords {
    private static String path = "/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/Reviews.csv";

    public static void main(String[] args) {
        JavaRDD<Row> rdd = LoadData.readCsvToRDD();

        long startTime = System.currentTimeMillis();

        JavaPairRDD<Integer, List<String>> time2summary =
                rdd.mapToPair(row -> new Tuple2<>(ParseTime.getYear((String)row.get(ConstantFields.Time)),
                        ParseText.getWordFromText((String)row.get(ConstantFields.Summary))));

        JavaPairRDD<Tuple2<Integer, String>, Long> year2words =
                time2summary.flatMapToPair(tuple -> tuple._2.stream().map(word -> new Tuple2<>(new Tuple2<>(tuple._1, word), 1L)).iterator());

        JavaPairRDD<Tuple2<Integer, String>, Long> count = year2words.reduceByKey((a, b) -> a +b);

        JavaPairRDD<Integer, Tuple2<String, Long>>  year2wordCount =
                count.mapToPair(tuple -> new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._1._2, tuple._2)));

        JavaPairRDD<Integer, Iterable<Tuple2<String, Long>>> allWords = year2wordCount.groupByKey().sortByKey();

        JavaPairRDD<Integer, List<Tuple2<String, Long>>> result =
                allWords.mapToPair(tuple -> new Tuple2<>(
                        tuple._1,
                        Lists.newArrayList(tuple._2).stream().
                                sorted(new TupleComparator()).limit(10).collect(toList()))).filter(tuple -> tuple._1!=0);

//        List<Tuple2<Integer, List<String>>> tuples = time2summary.collect();
        List<Tuple2<Integer, List<Tuple2<String, Long>>>> tuples = result.collect();

        System.out.println( "\n##########\t OUTPUT \t##########\n" );
        for (Tuple2<?,?> tuple : tuples) {
            System.out.println(tuple._1() + ": " + tuple._2().toString());
        }
        System.out.println( "\n##########\t END OUTPUT \t##########\n" );

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        System.out.println("\nTempo totale di esecuzione: " + totalTime + " sec");
    }


    static class TupleComparator implements Comparator<Tuple2<String, Long>>{
        @Override
        public int compare(Tuple2 t1, Tuple2 t2) {
            long t1Count = (long)t1._2;
            long t2Count = (long)t2._2;
            return -1*Long.compare(t1Count, t2Count);
        }
    }


}
