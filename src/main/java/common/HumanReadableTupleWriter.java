package common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HumanReadableTupleWriter extends TupleWriter {

  private BufferedWriter writer = null;
  private final Logger logger = LogManager.getLogger();

  public HumanReadableTupleWriter(String filePath, int tupleSize) {
    try {
      Path path = Paths.get(filePath);
      Files.createDirectories(path.getParent());
      this.writer = new BufferedWriter(new FileWriter(filePath));
    } catch (IOException e) {
      logger.error("Error creating HumanReadableTupleWriter: ", e);
    }
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
}
