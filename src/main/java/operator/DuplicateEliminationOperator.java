package operator;

import common.Tuple;

public class DuplicateEliminationOperator extends Operator {

  private final Operator inputOperator;
  private Tuple nextTuple;

  public DuplicateEliminationOperator(Operator operator) {
    super(operator.getOutputSchema());
    this.inputOperator = operator;
    this.nextTuple = this.inputOperator.getNextTuple();
  }

  @Override
  public void reset() {
    this.inputOperator.reset();
    this.nextTuple = this.inputOperator.getNextTuple();
  }

  @Override
  public Tuple getNextTuple() {

    Tuple toReturn = this.nextTuple;

    this.nextTuple = this.inputOperator.getNextTuple();

    while (nextTuple != null && nextTuple.equals(toReturn)) {
      this.nextTuple = this.inputOperator.getNextTuple();
    }

    return toReturn;
  }
}
