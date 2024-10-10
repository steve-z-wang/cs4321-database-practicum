package common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HumanReadableTupleWriter implements TupleWriter {

  private final int TUPLE_SIZE;

  private final BufferedWriter writer;
  private final Logger logger = LogManager.getLogger();

  public HumanReadableTupleWriter(String filePath, int tupleSize) throws IOException {
    this.TUPLE_SIZE = tupleSize;
    this.writer = new BufferedWriter(new FileWriter(filePath));
  }

  @Override
  public void writeTuple(Tuple tuple) {
    try {
      writer.write(tuple.toString());
      writer.newLine();
    } catch (IOException e) {
      logger.error("Error writing tuple: ", e);
    }
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      logger.error("Error closing HumanReadableTupleWriter: ", e);
    }
  }

  @Override
  public int getTupleSize() {
    return TUPLE_SIZE;
  }
}
