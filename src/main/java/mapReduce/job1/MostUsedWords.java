package mapReduce.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MostUsedWords {

    public static void main (String[] args) throws Exception {
        Job job  = new Job(new Configuration(), "Most Usec Words");
        job.setJarByClass(MostUsedWords.class);
        job.setMapperClass(MostUsedWordsMapper.class);
        //job.setCombinerClass(MostUsedWordsReducer.class);
        job.setReducerClass(MostUsedWordsReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}