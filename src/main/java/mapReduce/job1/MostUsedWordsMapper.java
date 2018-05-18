package mapReduce.job1;

import utilities.ConstantFields;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

public class MostUsedWordsMapper extends Mapper<Object, Text, IntWritable, Text> {

    private String regex = "[^A-Za-z0-9]";

    public void map (Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException
    {
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
        CSVRecord rec = parser.getRecords().get(0);

        if (!rec.get(ConstantFields.Id).equalsIgnoreCase("Id")) {
            // prendo l'anno della review
            String time = rec.get(ConstantFields.Time);

            try {
                long y = Long.valueOf(time) * 1000;
                Date date = new Date(y);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                IntWritable year = new IntWritable(calendar.get(Calendar.YEAR));

                // analizzo il summary della review
                String summary = rec.get(ConstantFields.Summary);
                String cleanSummary = summary.toLowerCase().replaceAll(regex, " ");
                StringTokenizer words = new StringTokenizer(cleanSummary);

                while (words.hasMoreTokens()) {
                    context.write(year, new Text(words.nextToken()));
                }
            } catch (NumberFormatException e) {
                context.getCounter(ConstantFields.COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }
        }

    }

}
