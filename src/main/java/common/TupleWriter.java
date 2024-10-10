package common;

public interface TupleWriter {
  void writeTuple(Tuple tuple);

  void close();

  int getTupleSize();
}
