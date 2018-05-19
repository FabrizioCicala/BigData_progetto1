package mapReduce.job2;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import utilities.ConstantFields;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilities.ParseTime;

public class ScorePerYearMapper extends Mapper<Object, Text, Text, YearScore> {

	public void map (Object key, Text value, Mapper<Object, Text, Text, YearScore>.Context context)
			throws IOException, InterruptedException
	{
		CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
		CSVRecord rec = parser.getRecords().get(0);

		if (!rec.get(ConstantFields.Id).equalsIgnoreCase("Id")) {
			String productId = rec.get(ConstantFields.ProductId);
			String time = rec.get(ConstantFields.Time);

			int year = ParseTime.getYear(time);
			if(year==0) context.getCounter(ConstantFields.COUNTERS.INVALID_RECORD_COUNT).increment(1L);
			else {
				if (year>2002 && year<2014) {
					int score = Integer.valueOf(rec.get(ConstantFields.Score));
					context.write(new Text(productId), new YearScore(year, score));
				}

			}

		}
	}
}
