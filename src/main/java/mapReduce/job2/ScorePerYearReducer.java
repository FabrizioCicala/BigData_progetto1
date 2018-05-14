package mapReduce.job2;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScorePerYearReducer extends Reducer<Text, YearScore, Text, Text> {

	public void reduce (Text key, Iterable<YearScore> values,
						Reducer<Text, YearScore, Text, Text>.Context context)
			throws IOException, InterruptedException
	{
		Map<Integer, LinkedList<Integer>> year2score = new HashMap<>();
		Map<Integer, Float> year2average = new HashMap<>();

		LinkedList<Integer> scores;
		for (YearScore val : values){

		    if (year2score.containsKey(val.getYear().get()))
				scores = year2score.get(val.getYear().get());
			else
                scores = new LinkedList<>();

			scores.add(val.getScore().get());
            year2score.put(val.getYear().get(), scores);
		
		}

		for (Integer year : year2score.keySet()){
			scores = year2score.get(year);
			float sumScores = 0;
			for (int score : scores)
				sumScores += score;
			float scale = 100;
			float media = (sumScores/(float)scores.size());
			float roudMedia = Math.round(media*scale)/scale;
			year2average.put(year, roudMedia);
		}

		context.write(key, new Text(year2average.toString()));

	}
}

