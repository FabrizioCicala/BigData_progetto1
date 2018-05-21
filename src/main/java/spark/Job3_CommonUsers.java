package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import utilities.ConstantFields;
import utilities.LoadData;

import java.util.List;

/**
 * 3. Un job in grado di generare coppie di prodotti che hanno almeno un utente in comune, ovvero
 * che sono stati recensiti da uno stesso utente, indicando, per ciascuna coppia, il numero di utenti
 * in comune. Il risultato deve essere ordinato in base allo ProductId del primo elemento della
 * coppia e, possibilmente, non deve presentare duplicati.
 * **/
public class Job3_CommonUsers {

    public static String commonUsers(JavaRDD<Row> rdd) {

        long startTime = System.currentTimeMillis();

        // PairRDD: prodotto, utente
        JavaPairRDD<String, String> product_user =
            rdd.mapToPair(row -> new Tuple2<>((String)row.get(ConstantFields.UserId), (String)row.get(ConstantFields.ProductId))).distinct();

        // PairRDD: utente, (prodotto 1, prodotto 2)
        JavaPairRDD<String, Tuple2<String, String>> user_prodCouple =
                product_user.join(product_user).filter(tuple -> tuple._2._1.compareTo(tuple._2._2)<0);

        // PairRDD: (prodotto 1, prodotto 2), count
        JavaPairRDD<String, Integer> commonUsers =
                user_prodCouple.mapToPair(tuple -> new Tuple2<>(tuple._2.toString(), 1))
                .reduceByKey((a, b) -> a+b).sortByKey();

//        commonUsers.coalesce(1, true).saveAsTextFile("/home/fabrizio/Scaricati/spark_job3_result");

        List<Tuple2<String, Integer>> tuples = commonUsers.collect();

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        return ("\nJob 3: " + totalTime + " sec");


    }
}
