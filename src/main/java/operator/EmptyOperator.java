package operator;

import common.Tuple;

public class EmptyOperator extends Operator {

  public EmptyOperator() {
    super(null);
  }

  @Override
  public Tuple getNextTuple() {
    return null; // Indicates no more tuples
  }

  @Override
  public void reset() {
    // No action needed since there are no tuples
  }
}
