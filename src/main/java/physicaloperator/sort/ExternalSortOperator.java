package physicaloperator.sort;

import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;
import physicaloperator.base.Operator;

public class ExternalSortOperator extends Operator {

  public ExternalSortOperator(
      Operator operator, List<OrderByElement> orderByElements, int sortBufferPages) {
    super(null);
  }

  @Override
  public void reset() {}

  @Override
  public Tuple getNextTuple() {
    return null;
  }
}
