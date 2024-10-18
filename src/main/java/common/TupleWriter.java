package common;

import java.util.List;

public abstract class TupleWriter {
  public abstract void writeTuple(Tuple tuple);

  public abstract void close();

  public void writeTuples(List<Tuple> tuples) {
    for (Tuple t : tuples) {
      writeTuple(t);
    }
  }
}
