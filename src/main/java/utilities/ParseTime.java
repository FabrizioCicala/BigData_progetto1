package utilities;

import java.util.Calendar;
import java.util.Date;

public class ParseTime {

    public static int getYear (String timeStamp) {
        try {
            long time = Long.parseLong(timeStamp)*1000L;
            Date date = new Date(time);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            return calendar.get(Calendar.YEAR);
        } catch ( NumberFormatException e){
            return 0;
        }
    }
}
