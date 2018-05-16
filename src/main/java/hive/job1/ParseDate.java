package hive.job1;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.util.Calendar;
import java.util.Date;

public class ParseDate extends UDF {

    public Text evaluate(Text text) {
        if(text == null) return null;
        try {
            long timestamp = Long.parseLong(text.toString());
            // timestamp*1000 is to convert seconds to milliseconds
            Date date = new Date(timestamp * 1000L);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            String year = Integer.toString(calendar.get(Calendar.YEAR));
            return new Text(year);
        } catch (NumberFormatException e){
            return null;
        }
    }
}