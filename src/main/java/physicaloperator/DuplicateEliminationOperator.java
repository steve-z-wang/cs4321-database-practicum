package physicaloperator;

import model.Tuple;

import java.util.List;

public class DuplicateEliminationOperator extends PhysicalOperator {

  private final PhysicalOperator childOperator;
  private Tuple nextTuple;

  public DuplicateEliminationOperator(PhysicalOperator operator) {
    super(operator.getOutputSchema());
    this.childOperator = operator;
    this.nextTuple = this.childOperator.getNextTuple();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Distinct");
    List<String> subplan = List.of(childOperator.toString().split("\n"));
    for(String s:subplan){
      sb.append("\n");
      sb.append("-").append(s);
    }
    return sb.toString();
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
