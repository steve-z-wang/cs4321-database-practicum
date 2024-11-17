package io.reader;

import java.util.ArrayList;
import java.util.List;

import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class TupleReader implements AutoCloseable {

  Logger logger = LogManager.getLogger(TupleReader.class);

  public abstract Tuple getNextTuple();

  public abstract void close();

  public abstract void reset();

  public void reset(int index) {
    logger.error("Not implemented");
  };

  public List<Tuple> getAllTuples() {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }
    return tuples;
  }
}
