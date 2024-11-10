package physicaloperator.join;

import model.Tuple;
import net.sf.jsqlparser.expression.Expression;
import physicaloperator.base.Operator;

public class SortMergeJoinOperator extends Operator {

  public SortMergeJoinOperator(
      Operator leftOperator, Operator rightOperator, Expression expression) {
    super(null);
  }

  @Override
  public void reset() {}

  @Override
  public Tuple getNextTuple() {
    return null;
  }
}
