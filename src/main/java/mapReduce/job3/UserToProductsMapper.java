package mapReduce.job3;

import utilities.ConstantFields;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class UserToProductsMapper extends Mapper<Object, Text, Text, Text> {

    public void map (Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException
    {
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        CSVRecord rec = parser.getRecords().get(0);

        if (!rec.get(ConstantFields.Id).equalsIgnoreCase("Id")) {
            // retrieve review user
            String user = rec.get(ConstantFields.UserId);

            // retrieve review product
            String product = rec.get(ConstantFields.ProductId);

            context.write(new Text(user), new Text(product));
        }

    }
}
