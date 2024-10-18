package physicaloperator;

import common.Tuple;

public class DuplicateEliminationOperator extends Operator {

  private final Operator childOperator;
  private Tuple nextTuple;

  public DuplicateEliminationOperator(Operator operator) {
    super(operator.getOutputSchema());
    this.childOperator = operator;
    this.nextTuple = this.childOperator.getNextTuple();
  }

  @Override
  public void reset() {
    this.childOperator.reset();
    this.nextTuple = this.childOperator.getNextTuple();
  }

  @Override
  public Tuple getNextTuple() {

    Tuple toReturn = this.nextTuple;

    this.nextTuple = this.childOperator.getNextTuple();

    while (nextTuple != null && nextTuple.equals(toReturn)) {
      this.nextTuple = this.childOperator.getNextTuple();
    }

    return toReturn;
  }
}
