package utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParseText {
    private static final String regex = "[^A-Za-z0-9]+";

    /** metodo che data una stringa restituisce la stringa "pulita",
     * cioè priva di simboli diversi da lettere e numeri **/
    public static String getCleanText (String text) {
        return text.toLowerCase().replaceAll(regex, " ").trim();
    }

    /** metodo che data una stringa restituisce la lista di parole in essa contenute **/
    public static List<String> getWordFromText (String text){
        if (text==null) return new ArrayList<>();
        return Arrays.asList(getCleanText(text).split(" "));

    }
}
