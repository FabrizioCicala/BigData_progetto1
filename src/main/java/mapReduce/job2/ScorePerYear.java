package mapReduce.job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ScorePerYear {
	public static void main (String[] args) throws Exception {
		
        Job job  = new Job(new Configuration(), "Score per Year");
        job.setJarByClass(ScorePerYear.class);
        job.setMapperClass(ScorePerYearMapper.class);
        job.setReducerClass(ScorePerYearReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(YearScore.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
	}
}
