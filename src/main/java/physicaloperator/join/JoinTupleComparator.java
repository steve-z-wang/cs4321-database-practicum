package physicaloperator.join;

import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import physicaloperator.TupleComparator;

public class JoinTupleComparator extends TupleComparator {
  private final List<Integer> rightOrderByIndices;

  public JoinTupleComparator(
      List<Column> leftSchema,
      List<OrderByElement> leftOrderByElements,
      List<Column> rightSchema,
      List<OrderByElement> rightOrderByElements) {
    super(leftSchema, leftOrderByElements);
    this.rightOrderByIndices = buildOrderedIndices(rightSchema, rightOrderByElements);

    if (orderByIndices.size() != rightOrderByIndices.size()) {
      throw new IllegalArgumentException(
          "Left and right schema order by elements must be the same size");
    }
  }

  @Override
  public int compare(Tuple leftTuple, Tuple rightTuple) {
    for (int i = 0; i < orderByIndices.size(); i++) {
      int leftValue = leftTuple.getElementAtIndex(orderByIndices.get(i));
      int rightValue = rightTuple.getElementAtIndex(rightOrderByIndices.get(i));
      if (leftValue != rightValue) {
        return Integer.compare(leftValue, rightValue);
      }
    }
    return 0;
  }
}
