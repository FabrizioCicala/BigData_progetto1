package mapReduce;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import java.io.*;
import java.nio.charset.Charset;

public class Prova {
    public static void mainp (String[] args) throws IOException {
        String path  = "/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/provaReviews.csv";
        File csv = new File(path);
        CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.DEFAULT);

    }

}
