package io.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HumanReadableTupleReader extends TupleReader {

  private static final Logger logger = LogManager.getLogger(HumanReadableTupleReader.class);

  private final String filePath;
  private BufferedReader reader;

  public HumanReadableTupleReader(String filePath) {
    this.filePath = filePath;

    try {
      this.reader = new BufferedReader(new FileReader(filePath));
    } catch (IOException e) {
      logger.error("Error creating HumanReadableTupleReader: ", e);
    }
  }

  @Override
  public Tuple getNextTuple() {
    try {
      String nextLine = this.reader.readLine();
      if (nextLine == null) {
        return null;
      }
      return new Tuple(nextLine);
    } catch (IOException e) {
      logger.error("Error reading next tuple: ", e);
      return null;
    }
  }

  @Override
  public void reset() {
    try {
      this.reader.close();
      reader = new BufferedReader(new FileReader(this.filePath));
    } catch (IOException e) {
      logger.error("Error resetting HumanReadableTupleReader: ", e);
    }
  }
}
