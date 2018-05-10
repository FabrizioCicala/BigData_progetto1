package mapReduce.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonUserMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);

    public void map (Object key, Text value,
                     Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException
    { context.write(value, one); }
}
