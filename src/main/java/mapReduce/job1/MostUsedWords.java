package mapReduce.job1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 1. Un job che sia in grado di generare, per ciascun anno, le dieci parole che sono state più usate
 * nelle recensioni (campo summary) in ordine di frequenza, indicando, per ogni parola, la sua
 * frequenza, ovvero il numero di occorrenze della parola nelle recensioni di quell’anno.
 **/
public class MostUsedWords {

    public static String mostUsedWord (String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        String input = args[0];
        String output = args[1]+"/job1";

        Job job  = new Job(new Configuration(), "Most Usec Words");
        job.setJarByClass(MostUsedWords.class);
        job.setMapperClass(MostUsedWordsMapper.class);
        //job.setCombinerClass(MostUsedWordsReducer.class);
        job.setReducerClass(MostUsedWordsReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        return ("Job 1: " + totalTime + " sec");

    }
}
