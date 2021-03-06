package utilities;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple2<?, ?>>, Serializable {

    public static int compareByTupleStringKey(Tuple2 t1, Tuple2 t2) {
        int comp = ((String) t1._1).compareTo((String) t2._1);
        if (comp==0){
            return -1*((String) t1._2).compareTo((String) t2._2);
        } else return -1*comp;
    }

    public static int compareByLongValues(Tuple2 t1, Tuple2 t2) {
        long t1Count = (long)t1._2;
        long t2Count = (long)t2._2;
        return -1*Long.compare(t1Count, t2Count); }

    public static int compareByYear(Tuple2 t1, Tuple2 t2) {
        int t1Count = (int)t1._1;
        int t2Count = (int)t2._1;
        return Integer.compare(t1Count, t2Count);
    }

    @Override
    public int compare(Tuple2<?, ?> tuple2, Tuple2<?, ?> t1) {
        return 0;
    }
}
