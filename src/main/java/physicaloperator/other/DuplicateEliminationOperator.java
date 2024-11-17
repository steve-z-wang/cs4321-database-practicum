package physicaloperator.other;

import model.Tuple;
import physicaloperator.base.PhysicalOperator;

public class DuplicateEliminationOperator extends PhysicalOperator {

  private final PhysicalOperator childOperator;
  private Tuple nextTuple;

  public DuplicateEliminationOperator(PhysicalOperator operator) {
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
