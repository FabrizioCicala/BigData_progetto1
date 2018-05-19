package mapReduce.job2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 2. Un job che sia in grado di generare, per ciascun prodotto, lo score medio ottenuto in ciascuno
 * degli anni compresi tra il 2003 e il 2012, indicando ProductId seguito da tutti gli score medi
 * ottenuti negli anni dell’intervallo. Il risultato deve essere ordinato in base al ProductId.
 **/
public class ScorePerYear {
	public static void main (String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
		
        Job job  = new Job(new Configuration(), "Score per Year");
        job.setJarByClass(ScorePerYear.class);
        job.setMapperClass(ScorePerYearMapper.class);
        job.setReducerClass(ScorePerYearReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(YearScore.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

//        Counter counter = job.getCounters().findCounter(utilities.ConstantFields.COUNTERS.INVALID_RECORD_COUNT);
//        System.out.println(counter.getDisplayName() + ": " + counter.getValue());

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        System.out.println("Tempo di esecuzione job 2 con mapReduce: " + totalTime + " sec");

    }
}
