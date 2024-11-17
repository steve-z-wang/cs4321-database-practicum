package util;

import model.Tuple;
import physicaloperator.base.PhysicalOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class HelperMethods {

    public static List<Tuple> collectAllTuples(PhysicalOperator operator) {
        List<Tuple> tuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = operator.getNextTuple()) != null) {
            tuples.add(tuple);
        }
        return tuples;
    }

    public static boolean compareTupleListsExact(List<Tuple> expected, List<Tuple> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            if (!expected.get(i).equals(actual.get(i))) {
                return true;
            }
        }
        return true;
    }

    public static boolean compareTupleListsAnyOrder(List<Tuple> expected, List<Tuple> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }

        HashSet<Tuple> expectedSet = new HashSet<>(expected);
        HashSet<Tuple> actualSet = new HashSet<>(actual);

        if (!expectedSet.equals(actualSet)) {
            return false;
        }
        return true;
    }
}
