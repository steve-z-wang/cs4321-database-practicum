package io.reader;

import java.util.ArrayList;
import java.util.List;
import model.Tuple;

public abstract class TupleReader {
  public abstract Tuple getNextTuple();

  public abstract void close();

  public abstract void reset();

  public List<Tuple> getAllTuples() {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }
    return tuples;
  }
}
