package hive.job1;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class CleanText extends UDF {
    private static final String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;.\\-:()?!\"']";

    public Text evaluate(Text text) {
        if(text == null) return null;
        String summary = text.toString();
        String cleanSummary = summary.toLowerCase().replaceAll(tokens, " ");
        return new Text(cleanSummary);
    }
}