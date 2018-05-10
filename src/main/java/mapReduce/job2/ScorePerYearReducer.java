package mapReduce.job2;

import java.io.IOException;
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
		Map<Integer, LinkedList<Integer>> countMap = new HashMap<>();
		Map<Integer, Float> countMap2 = new HashMap<>();

		LinkedList<Integer> scores = new LinkedList<>();
		for (YearScore val : values){
			if (countMap.containsKey(val.getYear().get())){
				scores = countMap.get(val.getYear().get());
				scores.add(val.getScore().get());
			} else
				scores.add(val.getScore().get());
		
			countMap.put(val.getYear().get(), scores);
		
		}

		for (Integer year : countMap.keySet()){
			scores = countMap.get(year);
			int sumScores = 0;
			for (int score : scores)
				sumScores += score;
			float media = sumScores/scores.size();
			countMap2.put(year, media);
		}

		context.write(key, new Text(countMap2.toString()));

	}
}

