package mapReduce.job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class UserToProductsReducer extends Reducer<Text, Text, Text, Text> {

    private Set<String> set;
    private List<String> couples;

    public void reduce (Text key, Iterable<Text> values,
                        Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException
    {
        set = new HashSet<>();
        // create a sorted set with product values
        for (Text val : values){
            set.add(val.toString());
        }

        couples = new ArrayList<>();
        createCouples();

        //Collections.sort(couples);

        for (String s : couples){
            context.write(new Text(s), null);
        }
    }

    private void createCouples(){
        ArrayList<String> products = new ArrayList<>(set);
        int i, j;
        for (i=0;i<products.size();i++) {
            for (j=i+1;j<products.size();j++){
                couples.add("["+products.get(i)+", "+products.get(j)+"]");
            }
        }
    }

}
