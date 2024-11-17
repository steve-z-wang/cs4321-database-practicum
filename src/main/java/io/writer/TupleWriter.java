package io.writer;

import java.util.List;
import model.Tuple;

public abstract class TupleWriter implements AutoCloseable {
  public abstract void writeTuple(Tuple tuple);

  public abstract void close();

  public void writeTuples(List<Tuple> tuples) {
    for (Tuple t : tuples) {
      writeTuple(t);
    }
  }
}
