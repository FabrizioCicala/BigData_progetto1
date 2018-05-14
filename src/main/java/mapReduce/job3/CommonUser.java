package mapReduce.job3;

import mapReduce.ConstantFields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonUser {
    public static void main (String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        // first mapReduce execution - UserToProducts
        Job job1  = new Job(new Configuration(), "Common User - first map reduce");
        job1.setJarByClass(CommonUser.class);
        job1.setMapperClass(UserToProductsMapper.class);
        //job.setCombinerClass();
        job1.setReducerClass(UserToProductsReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        // second mapReduce execution
        Job job2 = new Job(new Configuration(), "Common User - second map reduce");
        job2.setJarByClass(CommonUser.class);
        job2.setMapperClass(CommonUserMapper.class);
        //job.setCombinerClass();
        job2.setReducerClass(CommonUserReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/final_result"));

        job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime-startTime)/1000;
        System.out.println("Tempo di esecuzione job 3 con mapReduce: " + totalTime + " sec");

    }
}
