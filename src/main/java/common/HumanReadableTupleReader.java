package common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class HumanReadableTupleReader implements TupleReader {

  private final int TUPLE_SIZE;

  private String filePath;
  private BufferedReader reader;

  public HumanReadableTupleReader(String filePath, int tupleSize) throws IOException {
    this.TUPLE_SIZE = tupleSize;

    this.filePath = filePath;
    this.reader = new BufferedReader(new FileReader(filePath));
  }

  @Override
  public Tuple getNextTuple() throws IOException {
    String nextLine = this.reader.readLine();
    if (nextLine == null) {
      return null;
    }
    return new Tuple(nextLine);
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  @Override
  public void reset() throws IOException {
    this.reader.close();
    reader = new BufferedReader(new FileReader(this.filePath));
  }

  @Override
  public int getTupleSize() {
    return TUPLE_SIZE;
  }
}
