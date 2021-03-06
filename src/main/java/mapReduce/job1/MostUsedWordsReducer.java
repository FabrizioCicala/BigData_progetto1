package mapReduce.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.toMap;

public class MostUsedWordsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce (IntWritable key, Iterable<Text> values,
                         Reducer<IntWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException
    {
        Map<String, Integer> countMap = new HashMap<>();

        for (Text val : values){
            int freq = 1;
            if (countMap.containsKey(val.toString())){
               freq += countMap.get(val.toString());
           }
           countMap.put(val.toString(), freq);
        }

        // le entries della mappa vengono ordinate per valore e vengono prese le prima 10 coppie
        Map<String, Integer> sortedMap = countMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(10)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        for (String word : sortedMap.keySet())
            context.write(key, new Text(word+"\t"+sortedMap.get(word)));
    }


}
