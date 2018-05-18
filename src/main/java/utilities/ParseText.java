package utilities;

import java.util.Arrays;
import java.util.List;

public class ParseText {
    private static final String regex = "[^A-Za-z0-9]";
    public static List<String> getWordFromText (String text){
        String[] s = text.toLowerCase().replaceAll(regex, " ").trim().split(" ");
        return Arrays.asList(s);
    }
}
