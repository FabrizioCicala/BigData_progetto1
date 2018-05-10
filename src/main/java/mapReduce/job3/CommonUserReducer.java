package mapReduce.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonUserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce (Text key, Iterable<IntWritable> values,
                        Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException
    {
        int total = 0;
        for (IntWritable val : values){
            total += val.get();
        }
        context.write(key, new IntWritable(total));
    }
}
