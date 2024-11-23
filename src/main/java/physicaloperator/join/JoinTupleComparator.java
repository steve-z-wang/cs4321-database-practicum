package physicaloperator.join;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class JoinTupleComparator implements Comparator<Tuple> {

  // the order of left tuple index to check
  private final ArrayList<Integer> leftTupleIndexOrder;
  private final ArrayList<Integer> rightTupleIndexOrder;

  public JoinTupleComparator(
      List<Column> leftSchema,
      List<OrderByElement> leftOrderByElements,
      List<Column> rightSchema,
      List<OrderByElement> rightOrderByElements) {
    this.leftTupleIndexOrder = buildIndexOrder(leftSchema, leftOrderByElements);
    this.rightTupleIndexOrder = buildIndexOrder(rightSchema, rightOrderByElements);

    if (leftTupleIndexOrder.size() != rightTupleIndexOrder.size()) {
      throw new IllegalArgumentException(
          "Left and right schema order by elements must be the same size");
    }
  }

  /**
   * Builds a list of tuple indices based on the schema and order by elements.
   *
   * @param schema The schema containing column definitions
   * @param orderByElements The order by elements specifying which columns to include
   * @return List of indices corresponding to the ordered columns
   */
  private ArrayList<Integer> buildIndexOrder(
      List<Column> schema, List<OrderByElement> orderByElements) {
    ArrayList<Integer> indexOrder = new ArrayList<>();

    for (OrderByElement orderByElement : orderByElements) {
      Column orderColumn = (Column) orderByElement.getExpression();
      String orderColumnName = orderColumn.getFullyQualifiedName(true);

      for (int i = 0; i < schema.size(); i++) {
        if (schema.get(i).getFullyQualifiedName(true).equals(orderColumnName)) {
          indexOrder.add(i);
          break;
        }
      }
    }

    return indexOrder;
  }

  @Override
  public int compare(Tuple leftTuple, Tuple rightTuple) {
    for (int i = 0; i < leftTupleIndexOrder.size(); i++) {
      int leftValue = leftTuple.getElementAtIndex(leftTupleIndexOrder.get(i));
      int rightValue = rightTuple.getElementAtIndex(rightTupleIndexOrder.get(i));

      if (leftValue != rightValue) {
        return Integer.compare(leftValue, rightValue);
      }
    }
    return 0;
  }
}
