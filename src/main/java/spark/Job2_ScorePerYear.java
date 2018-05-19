package spark;

import static java.util.stream.Collectors.toList;

import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.google.common.collect.Lists;
import scala.Tuple2;
import utilities.ConstantFields;
import utilities.LoadData;
import utilities.ParseTime;
import utilities.TupleComparator;


public class Job2_ScorePerYear {

	/** 2. Un job che sia in grado di generare, per ciascun prodotto, lo score medio ottenuto in ciascuno
		 degli anni compresi tra il 2003 e il 2012, indicando ProductId seguito da tutti gli score medi
		 ottenuti negli anni dell’intervallo.
		 Il risultato deve essere ordinato in base al ProductId.*
     */

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        JavaRDD<Row> rdd = LoadData.readCsvToRDD();

        JavaRDD<Row> filtered_rows =
                rdd.filter(row -> ParseTime.getYear((String)row.get(ConstantFields.Time)) >= 2003 && ParseTime.getYear((String)row.get(ConstantFields.Time))<= 2012);

        JavaPairRDD<Tuple2<String,Integer>, Double> productIdYear2score =
                filtered_rows.mapToPair(row -> new Tuple2<>(new Tuple2<>((String) row.get(ConstantFields.ProductId),
                        ParseTime.getYear((String) row.get(ConstantFields.Time))),
                        Double.parseDouble((String) row.get(ConstantFields.Score))));

        JavaPairRDD<Tuple2<String,Integer>,List<Double>> productIdYear2Listscore =
                productIdYear2score.groupByKey().mapToPair(p -> new Tuple2<Tuple2<String,Integer>,List<Double>>(p._1(),Lists.newArrayList(p._2())));

        JavaPairRDD<Tuple2<String,Integer>,Double> prodIDYear2AVGScore =
                productIdYear2Listscore.mapToPair(p -> new Tuple2<>(p._1(),p._2().stream().mapToDouble(Double::doubleValue).sum() / p._2().size()));

        JavaPairRDD<String, Iterable<Tuple2<Integer,Double>>> prodID2YearAVGScore =
                prodIDYear2AVGScore.mapToPair(p -> new Tuple2<>(p._1._1, new Tuple2<>(p._1._2,p._2))).groupByKey().sortByKey();


        JavaPairRDD<String, List<Tuple2<Integer,Double>>> prodID2YearAVGScore_result =
                prodID2YearAVGScore.mapToPair(tuple -> new Tuple2<>(
                        tuple._1,
                        Lists.newArrayList(tuple._2).stream().sorted(TupleComparator::compareByYear).collect(toList())));


        prodID2YearAVGScore_result.coalesce(1, true).saveAsTextFile("/home/fabrizio/Scaricati/spark_job2_result");

//        List<Tuple2<String, List<Tuple2<Integer,Double>>>> tuples = prodID2YearAVGScore_result.collect();

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        System.out.println("\nTempo totale di esecuzione: " + totalTime + " sec");


    }

}