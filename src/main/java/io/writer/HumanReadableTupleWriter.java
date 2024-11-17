package io.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HumanReadableTupleWriter extends TupleWriter {

  private BufferedWriter writer = null;
  private final Logger logger = LogManager.getLogger();

  // Primary constructor
  private HumanReadableTupleWriter(BufferedWriter writer) {
    this.writer = writer;
  }

  // For existing files
  public HumanReadableTupleWriter(Path path) throws IOException {
    this(Files.newBufferedWriter(path, StandardOpenOption.WRITE));
  }

  // For creating new files
  public HumanReadableTupleWriter(String filePath) {
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
