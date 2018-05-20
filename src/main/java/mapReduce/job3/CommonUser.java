package mapReduce.job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 3. Un job in grado di generare coppie di prodotti che hanno almeno un utente in comune, ovvero
 * che sono stati recensiti da uno stesso utente, indicando, per ciascuna coppia, il numero di utenti
 * in comune. Il risultato deve essere ordinato in base allo ProductId del primo elemento della
 * coppia e, possibilmente, non deve presentare duplicati.
 **/
public class CommonUser {

    public static String commonUser (String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        String input = args[0];
        String output = args[1]+"/job3";

        // first mapReduce execution - UserToProducts
        // genera le coppie (utente, prodotto recensito dall'utente)
        Job job1  = new Job(new Configuration(), "Common User - first map reduce");
        job1.setJarByClass(CommonUser.class);
        job1.setMapperClass(UserToProductsMapper.class);
        //job.setCombinerClass();
        job1.setReducerClass(UserToProductsReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));

        job1.waitForCompletion(true);

        // second mapReduce execution
        // genera le triple (prodotto 1, prodotto 2, numero di utenti in comune)
        Job job2 = new Job(new Configuration(), "Common User - second map reduce");
        job2.setJarByClass(CommonUser.class);
        job2.setMapperClass(CommonUserMapper.class);
        //job.setCombinerClass();
        job2.setReducerClass(CommonUserReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(output+"/final_result"));

        job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        return ("Job 3: " + totalTime + " sec");

    }
}
