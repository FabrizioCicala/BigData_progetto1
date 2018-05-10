package mapReduce;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;

public class Prova {

    public static void main (String[] args) throws IOException {
        String path = "/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/provaReviews.csv";
        CSVParser parser = CSVParser.parse(new File(path), Charset.defaultCharset(), CSVFormat.DEFAULT);
        CSVRecord rec = parser.getRecords().get(1);
        System.out.println(rec.toString());

        if (!rec.get(ConstantFields.Id).equalsIgnoreCase("Id")) {
            String time = rec.get(ConstantFields.Time);
            Date date = new Date(Long.valueOf(time)*1000);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int year = calendar.get(Calendar.YEAR);

            int score = Integer.valueOf(rec.get(ConstantFields.Score));

            System.out.println(year);
            System.out.println(score);
        } else {
            System.out.println("Stai analizzando l'header del csv");
        }
    }
}
