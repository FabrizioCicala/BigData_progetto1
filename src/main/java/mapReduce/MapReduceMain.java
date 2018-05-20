package mapReduce;

import mapReduce.job1.MostUsedWords;
import mapReduce.job2.ScorePerYear;
import mapReduce.job3.CommonUser;

public class MapReduceMain {
    public static void maina (String[] args) throws Exception {
        String job1 = MostUsedWords.mostUsedWord(args);
        String job2 = ScorePerYear.scorePerYear(args);
        String job3 = CommonUser.commonUser(args);

        System.out.println(" Tempi di esecuzione MapReduce:");
        System.out.println(job1);
        System.out.println(job2);
        System.out.println(job3);
    }
}
