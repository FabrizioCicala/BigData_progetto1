package utilities;

import java.util.Calendar;
import java.util.Date;

public class ParseTime {

    /** metodo che data una stringa timeStamp che rappresenta una time stamp,
     *  restituisce l'anno **/
    public static int getYear (String timeStamp) {
        try {
            long time = Long.parseLong(timeStamp)*1000L;
            Date date = new Date(time);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            return calendar.get(Calendar.YEAR);
        } catch ( NumberFormatException e){
            return 0;   // torna 0 se il formato dell'anno non è coretto
        }
    }
}
