package common;

import java.io.IOException;

public interface TupleReader {
  Tuple getNextTuple() throws IOException;

  void close() throws IOException;

  void reset() throws IOException;

  int getTupleSize();
}
