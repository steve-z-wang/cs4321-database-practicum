package io.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class TupleReader {

  Logger logger = LogManager.getLogger(TupleReader.class);

  public abstract Tuple getNextTuple() throws IOException;

  public abstract void reset() throws IOException;

  public void reset(int index) throws IOException {
    logger.error("Not implemented");
  }
  ;

  public List<Tuple> getAllTuples() throws IOException {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }
    return tuples;
  }
}
